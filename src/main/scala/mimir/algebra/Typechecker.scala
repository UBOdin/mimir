package mimir.algebra;

import java.sql._;
import java.util.NoSuchElementException;
import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir.Database
import mimir.algebra.function._
import mimir.algebra.typeregistry._
import mimir.models.{Model, ModelManager}
import Arith.{Add, Sub, Mult, Div, And, Or, BitAnd, BitOr, ShiftLeft, ShiftRight}
import Cmp.{Gt, Lt, Lte, Gte, Eq, Neq, Like, NotLike}

class TypecheckError(msg: String, e: Throwable, context: Option[Operator] = None)
  extends Exception(msg, e)
{
  def errorTypeString =
    getClass().getTypeName()

  override def toString =
    context match {
      case None => s"$errorTypeString : $msg"
      case Some(oper) => s"$errorTypeString : $msg\n$oper"
    }


  override def getMessage =
    context match {
      case None => msg
      case Some(oper) => s"$msg in ${oper.toString.filter { _ != '\n' }.take(200)}"
    }
}

class MissingVariable(varName: String, e: Throwable, context: Option[Operator] = None)
  extends TypecheckError(varName, e, context);

/**
 * ExpressionChecker wraps around a bit of context that makes
 * recursion through Expression objects easier.  Concretely
 * 
 * `scope`: ... is a lookup function for the types of variables,
 *          which the Typechecker has no way to figure out on
 *          its own.  The easiest way to pull this off is to simply
 *          pass a Map[String,TAny] object, as its apply() method
 *          will do the trick, but it's handy to leave this open
 *          to any lookup function.  The scope doesn't need to be
 *          present for the typechecker to work, but if it isn't
 *          then it'll fail if it hits any Var object.
 *
 * `context` : ... is an operator for debugging purposes.  If
 *             any typechecker error occurs, then we'll annotate the
 *             error with this operator.
 */
class Typechecker(
  functions: Option[FunctionRegistry] = None, 
  aggregates: Option[AggregateRegistry] = None,
  types: TypeRegistry = DefaultTypeRegistry,
  models: Option[ModelManager] = None
) extends LazyLogging {
  /* Assert that the expressions claimed type is its type */
  def assert(e: Expression, t: Type, scope: (String => Type), context: Option[Operator] = None, msg: String = "Typechecker"): Unit = {
    val eType = typeOf(e, scope);
    if(!Typechecker.canCoerce(eType, t, types)){
      logger.trace(s"LUB: ${Typechecker.leastUpperBound(eType, t, types)}")
      throw new TypeException(eType, t, msg, Some(e))
    }
  }

  def weakTypeOf(e: Expression) =
    typeOf(e, (_) => TAny())

  def typeOf(e: Expression, o: Operator): Type =
    typeOf(e, scope = schemaOf(o).toMap, context = Some(o))

  def rootType = types.rootType _

  def typeOf(
    e: Expression, 
    scope: (String => Type) = { (_:String) => throw new RAException("Need a scope to typecheck expressions with variables") }, 
    context: Option[Operator] = None
  ): Type = {
    val recur = typeOf(_:Expression, scope, context)

    e match {
      case p: PrimitiveValue => p.getType;
      case Not(child) => assert(child, TBool(), scope, context, "NOT"); TBool()
      case p: Proc => p.getType(p.children.map { recur(_) }
                                          .map { types.rootType(_) }
                                )
      case Arithmetic(op, lhs, rhs) => 
        Typechecker.escalate(types.rootType(recur(lhs)), types.rootType(recur(rhs)), op, "Arithmetic", e)
      case Comparison(op, lhs, rhs) => {
        op match {
          case (Eq | Neq) => 
            Typechecker.assertLeastUpperBound(recur(lhs), recur(rhs), "Comparison", e, types)
          case (Gt | Gte | Lt | Lte) => 
            Typechecker.assertOneOf(
              types.rootType(
                Typechecker.assertLeastUpperBound(
                  recur(lhs), 
                  recur(rhs), 
                  "Comparison", 
                  e,
                  types
                )
              ),
              Set(TDate(), TInterval(), TTimestamp(), TInt(), TFloat()),
              e
            )
          case (Like | NotLike) =>
            assert(lhs, TString(), scope, context, "LIKE")
            assert(rhs, TString(), scope, context, "LIKE")
        }
        TBool()
      }
      case Var(name) => 
        try { 
          val t = scope(name)
          logger.debug(s"Type of $name is $t")
          t
        } catch {
          case x:NoSuchElementException => throw new MissingVariable(name, x, context)
        }
      case JDBCVar(t) => t
      case Function("CAST", fargs) =>
        // Special case CAST
        fargs(1) match {
          case TypePrimitive(t) => t
          case p:PrimitiveValue => { p match {
              case StringPrimitive(s) => types.typeForId(Integer.parseInt(s))
              case IntPrimitive(i)  =>  types.typeForId(i.toInt)
              case _ => throw new RAException("Invalid CAST to '"+p+"' of type: "+recur(p))
            }
          }
          case _ => TAny()
        }
      case Function(name, args) => 
        returnTypeOfFunction(name, args.map { recur(_) }.map { types.rootType(_) })

      case Conditional(condition, thenClause, elseClause) => 
        assert(condition, TBool(), scope, context, "WHEN")
        Typechecker.assertLeastUpperBound(
          recur(elseClause),
          recur(thenClause),
          "CASE-WHEN",
          e,
          types
        )
      case IsNullExpression(child) =>
        recur(child);
        TBool()
      case RowIdVar() => TRowId()
      case VGTerm(model, idx, args, hints) => 
        models match {
          case Some(registry) =>
            registry.get(model).varType(idx, 
              args.map { recur(_) }.map { types.rootType(_) }
            )
          case None => throw new RAException("Need Model Manager to typecheck expressions with VGTerms")
        }
    }
  }

  def returnTypeOfFunction(name: String, args: Seq[BaseType]): BaseType = {
    try {
      functions.flatMap { _.getOption(name) } match {
        case Some(NativeFunction(_, _, getType, _)) => 
          getType(args)
        case Some(ExpressionFunction(_, argNames, expr)) => 
          types.rootType(typeOf(expr, scope = argNames.zip(args).toMap))
        case Some(FoldFunction(_, expr)) =>
          args.tail.foldLeft(args.head){ case (curr,next) => 
            types.rootType(typeOf(expr, Map("CURR" -> curr, "NEXT" -> next))) }
        case None => 
          throw new RAException(s"Function $name(${args.mkString(",")}) is undefined")
      }
    } catch {
      case TypeException(found, expected, detail, None) =>
        throw TypeException(found, expected, detail, Some(Function(name, args.map{ TypePrimitive(_) })))
    }
  }

  def schemaOf(o: Operator): Seq[(String, Type)] =
  {
    o match {
      case Project(cols, src) =>
        val schema = schemaOf(src).toMap
        cols.map( { 
            case ProjectArg(col, expression) =>
              (col, typeOf(expression, scope = schema(_), context = Some(src)))
          })
      
      case ProvenanceOf(psel) => 
        // Not 100% sure this is kosher... doesn't ProvenanceOf introduce new columns?
        schemaOf(psel)
      
      case Annotate(subj,invisScm) => {
        schemaOf(subj)
      }
      
      case Recover(subj,invisScm) => {
        schemaOf(subj).union(invisScm.map(_._2).map(pasct => (pasct.name,pasct.typ)))
      }
      
      case Select(cond, src) => {
        val srcSchema = schemaOf(src);
        assert(cond, TBool(), srcSchema.toMap, Some(src), "SELECT")
        return srcSchema
      }

      case Aggregate(gbCols, aggCols, src) =>
        aggregates match {
          case None => throw new RAException("Need Aggregate Registry to typecheck aggregates")
          case Some(registry) => {

            /* Nested Typechecker */
            val srcSchema = schemaOf(src).toMap
            val chk = typeOf(_:Expression, scope = srcSchema, context = Some(src))

            /* Get Group By Args and verify type */
            val groupBySchema: Seq[(String, Type)] = gbCols.map(x => (x.toString, chk(x)))

            /* Get function name, check for AVG *//* Get function parameters, verify type */
            val aggSchema: Seq[(String, Type)] = aggCols.map(x => 
              (
                x.alias, 
                registry.typecheck(x.function, x.args.map { chk(_) }.map { types.rootType(_) })
              )
            )

            /* Send schema to parent operator */
            val sch = groupBySchema ++ aggSchema
            //println(sch)
            sch

          }

        }

      case Join(lhs, rhs) =>
        val lSchema = schemaOf(lhs);
        val rSchema = schemaOf(rhs);

        val overlap = lSchema.map(_._1).toSet & rSchema.map(_._1).toSet
        if(!(overlap.isEmpty)){
          throw new RAException("Ambiguous Keys ('"+overlap+"') in Cross Product\n"+o);
        }
        lSchema ++ rSchema

      case LeftOuterJoin(lhs, rhs, condition) =>
        schemaOf(Select(condition, Join(lhs, rhs)))

      case Union(lhs, rhs) =>
        val lSchema = schemaOf(lhs);
        val rSchema = schemaOf(rhs);

        if(!(lSchema.map(_._1).toSet.equals(rSchema.map(_._1).toSet))){
          throw new RAException("Schema Mismatch in Union\n"+o+s"$lSchema <> $rSchema");
        }
        lSchema

      case Table(_, _, sch, meta) => (sch ++ meta.map( x => (x._1, x._3) ))

      case View(_, query, _) => schemaOf(query)
      case AdaptiveView(_, _, query, _) => schemaOf(query)

      case HardTable(sch,_) => sch

      case Limit(_, _, src) => schemaOf(src)

      case Sort(_, src) => schemaOf(src)
    }
  }

  def baseSchemaOf(o: Operator): Seq[(String, BaseType)] = 
    schemaOf(o).map { case (name, t) => (name, types.rootType(t)) }
}

object Typechecker 
  extends LazyLogging
{

  val trivialTypechecker = new Typechecker()

  def assertNumeric(t: BaseType, e: Expression): BaseType =
  {
    if(t.isNumeric){
      throw new TypeException(t, TFloat(), "Numeric", Some(e))
    }
    t;
  }

  def canCoerce(from: BaseType, to: BaseType): Boolean =
  {
    logger.debug("Coerce from $from to $to")
    leastUpperBound(from, to) match {
      case Some(lub) => lub.equals(to)
      case None => false
    }
  }

  def canCoerce(from: Type, to: Type, types: TypeRegistry): Boolean =
  {
    logger.debug("Coerce from $from to $to")
    leastUpperBound(from, to, types) match {
      case Some(lub) => lub.equals(to)
      case None => false
    }
  }

  def leastUpperBound(a: BaseType, b: BaseType): Option[BaseType] =
  {
    if(a.equals(b)){ return Some(a); }
    (a, b) match {
      case (TAny(), _) => Some(b)
      case (_, TAny()) => Some(a)
      case (TInt(), TFloat()) => Some(TFloat())
      case (TFloat(), TInt()) => Some(TFloat())
      case (TDate(), TTimestamp()) => Some(TTimestamp())
      case (TTimestamp(), TDate()) => Some(TTimestamp())
      case (TRowId(), TString()) => Some(TRowId())
      case (TString(), TRowId()) => Some(TRowId())
      case (TRowId(), TInt()) => Some(TInt())
      case (TInt(), TRowId()) => Some(TInt())
      case _ => return None
    }
  }

  def leastUpperBound(tl: TraversableOnce[BaseType]): Option[BaseType] =
  {
    tl.map { Some(_) }.fold(Some(TAny()):Option[BaseType]) { case (Some(a), Some(b)) => leastUpperBound(a, b) case _ => None }
  }

  def assertLeastUpperBound(a: BaseType, b: BaseType, msg: String, e: Expression): BaseType =
  {
    leastUpperBound(a, b) match {
      case Some(t) => t
      case None => throw new TypeException(a, b, msg, Some(e))
    }
  }
  def assertLeastUpperBound(tl: TraversableOnce[BaseType], msg: String, e: Expression): BaseType =
  {
    tl.fold(TAny()) { assertLeastUpperBound(_, _, msg, e) }
  }

  def assertOneOf(a: BaseType, candidates: TraversableOnce[BaseType], e: Expression): BaseType =
  {
    candidates.flatMap { leastUpperBound(a, _) }.collectFirst { case x => x } match {
      case Some(t) => t
      case None => 
        throw new TypeException(a, TAny(), s"Not one of $candidates", Some(e))
    }
  }

  def leastUpperBound(a: Type, b: Type, types:TypeRegistry): Option[Type] =
  {
    (a, b) match {
      case (TUser(_), TUser(_))             => leastUpperBound(types.parentType(a), b, types)
                                                .map { Some(_) }
                                                .getOrElse{ leastUpperBound(a, types.parentType(b), types) }
      case (TUser(_), _:BaseType)           => leastUpperBound(types.parentType(a), b, types)
      case (_:BaseType, TUser(_))           => leastUpperBound(a, types.parentType(b), types)
      case (aBase:BaseType, bBase:BaseType) => leastUpperBound(aBase, bBase)
    }
  }

  def leastUpperBound(tl: TraversableOnce[Type], types:TypeRegistry): Option[Type] =
  {
    tl.map { Some(_) }.fold(Some(TAny()):Option[Type]) { case (Some(a), Some(b)) => leastUpperBound(a, b, types) case _ => None }
  }

  def assertLeastUpperBound(a: Type, b: Type, msg: String, e: Expression, types:TypeRegistry): Type =
  {
    leastUpperBound(a, b, types) match {
      case Some(t) => t
      case None => throw new TypeException(a, b, msg, Some(e))
    }
  }
  def assertLeastUpperBound(tl: TraversableOnce[Type], msg: String, e: Expression, types:TypeRegistry): Type =
  {
    tl.fold(TAny()) { assertLeastUpperBound(_, _, msg, e, types) }
  }

  def escalate(a: BaseType, b: BaseType, op: Arith.Op, msg: String, e: Expression): BaseType = 
  {
    escalate(a, b, op) match {
      case Some(t) => t
      case None => throw new TypeException(a, b, msg, Some(e));
    }
  }
  def escalate(a: BaseType, b: BaseType, op: Arith.Op): Option[BaseType] = 
  {
    // Start with special case overrides
    (a, b, op) match {
      case (TDate() | TTimestamp(), 
            TDate() | TTimestamp(), 
            Arith.Add)                => return Some(TDate())
      // Interval Arithmetic
      case (TDate() | TTimestamp(), 
            TDate() | TTimestamp(), 
            Arith.Sub)                => return Some(TInterval())
      case (TDate() | TTimestamp() | TInterval(), 
            TInterval(), 
            Arith.Sub | Arith.Add)    => return Some(a)
      case (TInt() | TFloat(), TInterval(), Arith.Mult) | 
           (TInterval(), TInt() | TFloat(), Arith.Mult | Arith.Div)  
                                        => return Some(TInterval())
      case (TInterval(), TInterval(), Arith.Div)
                                            => return Some(TFloat())

      // TAny() cases
      case (TAny(), TAny(), _)        => return Some(TAny())
      case (TAny(), TDate() | TTimestamp(), 
            Arith.Sub)                => Some(TInterval())
      case (TDate() | TTimestamp(), TAny(), 
            Arith.Sub)                => Some(TAny()) // Either TInterval or TDate, depending
      case _ => ()
    }

    (op) match {
      case (Arith.Add | Arith.Sub | Arith.Mult | Arith.Div) => 
        if( (a.isNumeric || a == TAny()) && (b.isNumeric || b == TAny()) ){
          leastUpperBound(a, b)
        } else {
          None
        }

      case (Arith.BitAnd | Arith.BitOr | Arith.ShiftLeft | Arith.ShiftRight) =>
        (a, b) match {
          case (TInt() | TAny(), TInt() | TAny()) => Some(TInt())
          case _ => None
        }

      case (Arith.And | Arith.Or) =>
        (a, b) match {
          case (TBool() | TAny(), TBool() | TAny()) => Some(TBool())
          case _ => None
        }
    }
  }
  def escalate(a: Option[BaseType], b: Option[BaseType], op: Arith.Op): Option[BaseType] =
  {
    (a, b) match {
      case (None,_) => b
      case (_,None) => a
      case (Some(at), Some(bt)) => escalate(at, bt, op)
    }
  }

  def escalate(l: TraversableOnce[BaseType], op: Arith.Op): Option[BaseType] =
  {
    l.map(Some(_)).fold(None)(escalate(_,_,op))
  }
  def escalate(l: TraversableOnce[BaseType], op: Arith.Op, msg: String, e: Expression): BaseType =
  {
    l.fold(TAny())(escalate(_,_,op,msg,e))
  }
}
