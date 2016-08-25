package mimir.algebra;

import java.sql._;

import mimir.algebra.Type._
import mimir.provenance.Provenance
import mimir.ctables.{VGTerm, CTables}
import mimir.optimizer.ExpressionOptimizer

/**
 * A placeholder for use in extending Eval;  A proc is an expression that 
 * can be evaluated, but is not itself part of mimir's grammar.
 * 
 * The proc defines the method of evaluation.
 */
abstract class Proc(args: List[Expression]) extends Expression
{
  def getType(argTypes: List[Type.T]): Type.T
  def getArgs = args
  def children = args
  def get(v: List[PrimitiveValue]): PrimitiveValue
}


object Eval 
{

  val SAMPLE_COUNT = 100

  /**
   * Evaluate the specified expression and cast the result to an Long
   */
  def evalInt(e: Expression) =
    eval(e).asLong
  /**
   * Evaluate the specified expression and cast the result to a String
   */
  def evalString(e: Expression) =
    eval(e).asString
  /**
   * Evaluate the specified expression and cast the result to a Double
   */
  def evalFloat(e: Expression) =
    eval(e).asDouble
  /**
   * Evaluate the specified expression and cast the result to a Boolean
   */
  def evalBool(e: Expression, bindings: Map[String, PrimitiveValue] = Map[String, PrimitiveValue]()): Boolean =
    eval(e, bindings) match {
      case BoolPrimitive(v) => v

      /* TODO Need to check if this is allowed? */
      case v: NullPrimitive => false

      case v => throw new TypeException(TBool, v.getType, "Cast")
    }
  /**
   * Evaluate the specified expression and return the primitive value
   */
  def eval(e: Expression): PrimitiveValue = 
    eval(e, Map[String, PrimitiveValue]())

  /**
   * Evaluate the specified expression given a set of Var/Value bindings
   * and return the primitive value of the result
   */
  def eval(e: Expression, 
           bindings: Map[String, PrimitiveValue]
  ): PrimitiveValue = 
  {
    if(e.isInstanceOf[PrimitiveValue]){
      return e.asInstanceOf[PrimitiveValue]
    } else {
      e match {
        case Var(v) => bindings.get(v) match {
          case None => throw new SQLException("Variable Out Of Scope: "+v+" (in "+bindings+")");
          case Some(s) => s
        }
        case Arithmetic(op, lhs, rhs) =>
          applyArith(op, eval(lhs, bindings), eval(rhs, bindings))
        case Comparison(op, lhs, rhs) =>
          applyCmp(op, eval(lhs, bindings), eval(rhs, bindings))
        case Conditional(condition, thenClause, elseClause) =>
          if(evalBool(condition, bindings)) { eval(thenClause, bindings) }
          else                              { eval(elseClause, bindings) }
        case Not(NullPrimitive()) => NullPrimitive()
        case Not(c) => BoolPrimitive(!evalBool(c, bindings))
        case p:Proc => {
          p.get(p.getArgs.map(eval(_, bindings)))
        }
        case IsNullExpression(c) => {
          val isNull: Boolean = 
            eval(c, bindings).
            isInstanceOf[NullPrimitive];
          return BoolPrimitive(isNull);
        }
        case Function(op, params) => {
          op.toUpperCase match {
            case "ABSOLUTE" => eval(params(0), bindings) match {
              case IntPrimitive(i) => if(i < 0){ IntPrimitive(-i) } else { IntPrimitive(i) }
              case FloatPrimitive(f) => if(f < 0){ FloatPrimitive(-f) } else { FloatPrimitive(f) }
              case NullPrimitive() => NullPrimitive()
              case x => throw new SQLException("Non-numeric parameter to absolute: '"+x+"'")
            }
            case "MIMIR_MAKE_ROWID" => Provenance.joinRowIds(params.map(x => eval(x, bindings)))
            case "DATE" | "TO_DATE" =>
              val date = params.head.asInstanceOf[StringPrimitive].v.split("-").map(x => x.toInt)
              new DatePrimitive(date(0), date(1), date(2))
            case "CAST" => {
              try {
                Eval.eval(params(1), bindings) match {
                  case TypePrimitive(TInt) => IntPrimitive(Eval.eval(params(0), bindings).asLong)
                  case TypePrimitive(TFloat) => FloatPrimitive(Eval.eval(params(0), bindings).asDouble)
                  case TypePrimitive(TString) => StringPrimitive(Eval.eval(params(0), bindings).asString)
                  case x => throw new SQLException("Unknown cast type: '"+x+"'")
                }
              } catch {
                case _:TypeException=> NullPrimitive();
                case _:NumberFormatException => NullPrimitive();
              }
            }

            case "__LIST_MIN" =>
              new FloatPrimitive(params.map(x => {
                try {
                  eval(x).asDouble
                } catch {
                  case e:Throwable => Double.MaxValue
                }
              }).min) // TODO Generalized Comparator
            case "__LIST_MAX" =>
              new FloatPrimitive(params.map(x => {
                try {
                  eval(x).asDouble
                } catch {
                  case e:Throwable => Double.MinValue
                }
              }).max) // TODO Generalized Comparator
            case CTables.VARIANCE => {
              var variance = 0.0
              try {
                val (sum, samples) = sampleExpression(params(0))
                val mean = sum/SAMPLE_COUNT
                for(i <- samples.keys){
                  variance += (i - mean) * (i - mean) * samples(i)
                }
                FloatPrimitive(variance/SAMPLE_COUNT)
              } catch {
                case e: TypeException => new NullPrimitive()
              }
            }
            case CTables.CONFIDENCE => {
              var variance = 0.0
              try {
                val (_, samples) = sampleExpression(params(0))
                val percentile = params(1).asInstanceOf[PrimitiveValue].asDouble
                val keys = samples.keys.toList.sorted
                var count = 0
                var i = -1
                while(count < percentile){
                  i += 1
                  count += samples(keys(i))
                }
                val med = keys(i)
                for(i <- samples.keys){
                  variance += (i - med) * (i - med) * samples(i)
                }
                val conf = Math.sqrt(variance/SAMPLE_COUNT) * 1.96
                StringPrimitive((med - conf).formatted("%.2f") + " | " + (med + conf).formatted("%.2f"))
              } catch {
                case e: TypeException => new NullPrimitive()
              }
            }
//            case fn => throw new SQLException("Unknown Function: "+fn)
          }
        }
      }
    }
  }

  def sampleExpression(exp: Expression): (Double, collection.mutable.Map[Double, Int]) = {
    var sum  = 0.0
    val samples = collection.mutable.Map[Double, Int]()
    for( i <- 0 until SAMPLE_COUNT) {
      val bindings = Map[String, IntPrimitive]("__SEED" -> IntPrimitive(i+1))
      val sample =
        try {
          Eval.eval(exp, bindings).asDouble
        } catch {
          case e: Exception => 0.0
        }
      sum += sample
      if(samples.contains(sample))
        samples(sample) = samples(sample) + 1
      else
        samples += (sample -> 1)
    }
    (sum, samples)
  }

  /**
   * Apply one level of simplification to the passed expression.  Typically
   * this method should not be used directly, but is invoked as part of
   * either inline() method.
   *
   * If the expression is independent of VGTerms or variable references
   * then the expression can be deterministically evaluated outright.
   * In this case, simplify() returns the PrimitiveValue that the 
   * expression evaluates to.  
   *
   * CASE statements are simplified further.  See simplifyCase()
   */
  def simplify(e: Expression): Expression = {
    // println("Simplify: "+e)
    ExpressionOptimizer.optimize(
      if(ExpressionUtils.getColumns(e).isEmpty && 
         !CTables.isProbabilistic(e)) 
      { 
        try {
          eval(e) 
        } catch {
          case _:MatchError => 
            e.rebuild(e.children.map(simplify(_)))
        }
      } else e match { 
        case Conditional(condition, thenClause, elseClause) =>
          val conditionValue = simplify(condition)
          if(conditionValue.isInstanceOf[BoolPrimitive]){
            if(conditionValue.asInstanceOf[BoolPrimitive].v){
              simplify(thenClause)
            } else {
              simplify(elseClause)
            }
          } else { e.rebuild(e.children.map(simplify(_))) }
        case Arithmetic(Arith.And, lhs, rhs) => 
          ExpressionUtils.makeAnd(simplify(lhs), simplify(rhs))
        case Arithmetic(Arith.Or, lhs, rhs) => 
          ExpressionUtils.makeOr(simplify(lhs), simplify(rhs))
        case _ => e.rebuild(e.children.map(simplify(_)))
      }
    )
  }

  /**
   * Thoroughly inline an expression, recursively applying simplify()
   * at levels, to all subtrees of the expression.
   */
  def inline(e: Expression): Expression = 
    inline(e, Map[String, Expression]())
  /**
   * Apply a given variable binding to the specified expression, and then
   * thoroughly inline it, recursively applying simplify() at all levels,
   * to all subtrees of the expression
   */
  def inline(e: Expression, bindings: Map[String, Expression]):
    Expression = 
  {
    e match {
      case Var(v) => bindings.get(v).getOrElse(Var(v))
      case _ => 
        simplify( e.rebuild( e.children.map( inline(_, bindings) ) ) )

    }
  }

  def inlineWithoutSimplifying(e: Expression, bindings: Map[String,Expression]):
    Expression =
  {
    e match {
      case Var(v) => bindings.get(v).getOrElse(Var(v))
      case _ => 
        e.rebuild( e.children.map( inlineWithoutSimplifying(_, bindings) ) ) 
    }
  }
  
  /**
   * Perform arithmetic on two primitive values.
   */
  def applyArith(op: Arith.Op, 
            a: PrimitiveValue, b: PrimitiveValue
  ): PrimitiveValue = {
    if(a.isInstanceOf[NullPrimitive] || 
       b.isInstanceOf[NullPrimitive]){
      (op, a, b) match {
        case (Arith.And, NullPrimitive(), BoolPrimitive(false)) 
           | (Arith.And, BoolPrimitive(false), NullPrimitive()) => 
          BoolPrimitive(false)
        case (Arith.Or, NullPrimitive(), BoolPrimitive(true)) 
           | (Arith.Or, BoolPrimitive(true), NullPrimitive()) => 
          BoolPrimitive(true)
        case _ => NullPrimitive()
      }
    } else {
      (op, Typechecker.escalate(a.getType, b.getType)) match { 
        case (Arith.Add, TInt) => 
          IntPrimitive(a.asLong + b.asLong)
        case (Arith.Add, TFloat) => 
          FloatPrimitive(a.asDouble + b.asDouble)
        case (Arith.Sub, TInt) => 
          IntPrimitive(a.asLong - b.asLong)
        case (Arith.Sub, TFloat) => 
          FloatPrimitive(a.asDouble - b.asDouble)
        case (Arith.Mult, TInt) => 
          IntPrimitive(a.asLong * b.asLong)
        case (Arith.Mult, TFloat) => 
          FloatPrimitive(a.asDouble * b.asDouble)
        case (Arith.Div, (TFloat|TInt)) => 
          FloatPrimitive(a.asDouble / b.asDouble)
        case (Arith.And, TBool) => 
          BoolPrimitive(
            a.asInstanceOf[BoolPrimitive].v &&
            b.asInstanceOf[BoolPrimitive].v
          )
        case (Arith.Or, TBool) => 
          BoolPrimitive(
            a.asInstanceOf[BoolPrimitive].v ||
            b.asInstanceOf[BoolPrimitive].v
          )
      }
    }
  }

  /**
   * Perform a comparison on two primitive values.
   */
  def applyCmp(op: Cmp.Op, 
            a: PrimitiveValue, b: PrimitiveValue
  ): PrimitiveValue = {
    if(a.isInstanceOf[NullPrimitive] || 
       b.isInstanceOf[NullPrimitive]){
      NullPrimitive()
    } else {
      op match { 
        case Cmp.Eq => 
          BoolPrimitive(a.payload.equals(b.payload))
        case Cmp.Neq => 
          BoolPrimitive(!a.payload.equals(b.payload))
        case Cmp.Gt => 
          Typechecker.escalate(a.getType, b.getType, "Eval", Comparison(op, a, b)) match {
            case TInt => BoolPrimitive(a.asLong > b.asLong)
            case TFloat => BoolPrimitive(a.asDouble > b.asDouble)
            case TDate => 
              BoolPrimitive(
                a.asInstanceOf[DatePrimitive].
                 compare(b.asInstanceOf[DatePrimitive])<0
              )
          }
        case Cmp.Gte => 
          Typechecker.escalate(a.getType, b.getType, "Eval", Comparison(op, a, b)) match {
            case TInt => BoolPrimitive(a.asLong >= b.asLong)
            case TFloat => BoolPrimitive(a.asDouble >= b.asDouble)
            case TDate => 
              BoolPrimitive(
                a.asInstanceOf[DatePrimitive].
                 compare(b.asInstanceOf[DatePrimitive])<=0
              )
            case TBool => BoolPrimitive(a match {
              case BoolPrimitive(true) => true
              case BoolPrimitive(false) => {
                b match {
                  case BoolPrimitive(true) => false
                  case _ => true
                }
              }
            })
          }
        case Cmp.Lt => 
          Typechecker.escalate(a.getType, b.getType, "Eval", Comparison(op, a, b)) match {
            case TInt => BoolPrimitive(a.asLong < b.asLong)
            case TFloat => BoolPrimitive(a.asDouble < b.asDouble)
            case TDate => 
              BoolPrimitive(
                a.asInstanceOf[DatePrimitive].
                 compare(b.asInstanceOf[DatePrimitive])>0
              )
          }
        case Cmp.Lte => 
          Typechecker.escalate(a.getType, b.getType, "Eval", Comparison(op, a, b)) match {
            case TInt => BoolPrimitive(a.asLong <= b.asLong)
            case TFloat => BoolPrimitive(a.asDouble <= b.asDouble)
            case TDate => 
              BoolPrimitive(
                a.asInstanceOf[DatePrimitive].
                 compare(b.asInstanceOf[DatePrimitive])>=0
              )
          }
      }
    }
  }
}