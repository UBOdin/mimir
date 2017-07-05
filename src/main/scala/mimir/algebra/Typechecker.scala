package mimir.algebra;

import java.sql._;
import java.util.NoSuchElementException;
import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir.Database
import mimir.algebra.function._
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
	models: Option[ModelManager] = None
) extends LazyLogging {
	/* Assert that the expressions claimed type is its type */
	def assert(e: Expression, t: Type, scope: (String => Type), context: Option[Operator] = None, msg: String = "Typechecker"): Unit = {
		val eType = typeOf(e, scope);
		if(Typechecker.escalate(eType, t, msg, e) != t){
			throw new TypeException(eType, t, msg, Some(e))
		}
	}

	def weakTypeOf(e: Expression) =
		typeOf(e, (_) => TAny())

	def typeOf(e: Expression, o: Operator): Type =
		typeOf(e, scope = schemaOf(o).toMap, context = Some(o))

	def typeOf(
		e: Expression, 
		scope: (String => Type) = { (_:String) => throw new RAException("Need a scope to typecheck expressions with variables") }, 
		context: Option[Operator] = None
	): Type = {
		val recur = typeOf(_:Expression, scope, context)

		e match {
			case p: PrimitiveValue => p.getType;
			case Not(child) => assert(child, TBool(), scope, context, "NOT"); TBool()
			case p: Proc => p.getType(p.children.map(recur(_)))
			case Arithmetic(op, lhs, rhs) => {
				op match {
					case (Add | Sub | Mult | Div | BitAnd | BitOr | ShiftLeft | ShiftRight) => 
					  Typechecker.assertNumeric(Typechecker.escalate(recur(lhs), recur(rhs), op.toString, e), e);
					case (And | Or) => 
						assert(lhs, TBool(), scope, context, "BoolOp");
						assert(rhs, TBool(), scope, context, "BoolOp");
						TBool()
				}
			}
			case Comparison(op, lhs, rhs) => {
				op match {
					case (Eq | Neq) => 
						Typechecker.escalate(recur(lhs), recur(rhs), "Comparison", e);
					case (Gt | Gte | Lt | Lte) => 
						if(recur(lhs) != TDate() && recur(rhs) != TDate()) {
							Typechecker.assertNumeric(Typechecker.escalate(recur(lhs), recur(rhs), "Comparison", e), e)
						}
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
                    case StringPrimitive(s) => Type.toSQLiteType(Integer.parseInt(s))
                    case IntPrimitive(i)  =>  Type.toSQLiteType(i.toInt)
                    case _ => throw new SQLException("Invalid CAST to '"+p+"' of type: "+recur(p))
                }

			case Function(name, args) =>
				returnTypeOfFunction(name, args.map { recur(_) })

			case Conditional(condition, thenClause, elseClause) => 
				assert(condition, TBool(), scope, context, "WHEN")
				Typechecker.escalate(
					recur(elseClause),
					recur(thenClause),
					"IF, ELSE CLAUSE", e
				)
			case IsNullExpression(child) =>
				recur(child);
				TBool()
			case RowIdVar() => TRowId()
			case VGTerm(model, idx, args, hints) => 
				models match {
					case Some(registry) =>
						registry.get(model).varType(idx, args.map(recur(_)))
					case None => throw new RAException("Need Model Manager to typecheck expressions with VGTerms")
				}
    }
  }

	def returnTypeOfFunction(name: String, args: Seq[Type]): Type = {
    try {
      functions.flatMap { _.getOption(name) } match {
        case Some(NativeFunction(_, _, getType)) => 
          getType(args)
        case Some(ExpressionFunction(_, argNames, expr)) => 
          typeOf(expr, scope = argNames.zip(args).toMap)
        case Some(FoldFunction(_, expr)) =>
          args.tail.foldLeft(args.head){ case (curr,next) => 
            typeOf(expr, Map("CURR" -> curr, "NEXT" -> next)) }
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
								registry.typecheck(x.function, x.args.map(chk(_)))
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
					throw new SQLException("Ambiguous Keys ('"+overlap+"') in Cross Product\n"+o);
				}
				lSchema ++ rSchema

			case LeftOuterJoin(lhs, rhs, condition) =>
				schemaOf(Select(condition, Join(lhs, rhs)))

			case Union(lhs, rhs) =>
				val lSchema = schemaOf(lhs);
				val rSchema = schemaOf(rhs);

				if(!(lSchema.map(_._1).toSet.equals(rSchema.map(_._1).toSet))){
					throw new SQLException("Schema Mismatch in Union\n"+o);
				}
				lSchema

			case Table(_, _, sch, meta) => (sch ++ meta.map( x => (x._1, x._3) ))

			case View(_, query, _) => schemaOf(query)

			case EmptyTable(sch) => sch

			case Limit(_, _, src) => schemaOf(src)

			case Sort(_, src) => schemaOf(src)
		}
	}
}

object Typechecker 
{


	def assertNumeric(t: Type, e: Expression): Type =
 	{
		if(!Type.isNumeric(t)){
			throw new TypeException(t, TFloat(), "Numeric", Some(e))
 		}
 		t;
 	}

 	def escalateLeft(a: Type, b: Type): Option[Type] =
 	{
		(a,b) match {
			case _ if a.equals(b)         => Some(a)
			case (TUser(name),_)          => escalateLeft(TypeRegistry.baseType(name),b)
			case (TAny(),_)               => Some(b)
			case (_,TAny())               => Some(a)
			case (TInt(),TFloat())        => Some(TFloat())
			case (TDate(), TTimestamp())  => Some(TTimestamp())
			case (TRowId(),TString())     => Some(b)
			case _                        => None
		}
 	}

	def escalate(a: Type, b: Type, msg: String, e: Expression): Type = 
		escalate(a, b, msg, Some(e))
	def escalate(a: Type, b: Type, msg: String, e: Option[Expression]): Type = 
	{
		escalate(a, b) match {
			case Some(t) => t
			case None => throw new TypeException(a, b, msg, e);
		}
	}
	def escalate(a: Type, b: Type): Option[Type] = 
	{
		escalateLeft(a, b) match {
			case s@Some(_) => s
			case None   => escalateLeft(b, a)
		}
	}
	def escalate(a: Option[Type], b: Option[Type]): Option[Type] =
	{
		(a, b) match {
			case (None,_) => b
			case (_,None) => a
			case (Some(at), Some(bt)) => escalate(at, bt)
		}
	}

	def escalate(l: TraversableOnce[Type]): Option[Type] =
	{
		l.map(Some(_)).fold(None)(escalate(_,_))
	}
	def escalate(l: TraversableOnce[Type], msg: String, e: Expression): Type =
	{
		l.fold(TAny())(escalate(_,_,msg,e))
	}
}