package mimir.algebra;

import java.sql._;

import mimir.Database
import mimir.algebra.function._
import mimir.provenance.Provenance
import mimir.ctables.CTables

class EvalTypeException(msg: String, e: Expression, v: PrimitiveValue) extends Exception

class Eval(
  functions: Option[FunctionRegistry] = None
)
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

      case v => throw new TypeException(TBool(), v.getType, "Cast")
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
    e match {
      case p : PrimitiveValue => p
      case Var(v) => bindings.get(v) match {
        case None => throw new RAException("Variable Out Of Scope: "+v+" (in "+bindings+")");
        case Some(s) => s
      }
      case RowIdVar() => throw new RAException("Evaluating RowIds in the Interpreter Unsupported")
      case JDBCVar(t) => throw new RAException("Evaluating JDBCVars in the Interpreter Unsupported")
      case v:VGTerm => throw new RAException(s"Evaluating VGTerms ($v) in the Interpreter Unsupported")
      // Special case And/Or arithmetic to enable shortcutting
      case Arithmetic(Arith.And, lhs, rhs) =>
        eval(lhs, bindings) match {
          case BoolPrimitive(false) => BoolPrimitive(false)
          case BoolPrimitive(true) => eval(rhs, bindings)
          case NullPrimitive() => 
            eval(rhs, bindings) match {
              case BoolPrimitive(false) => BoolPrimitive(false)
              case NullPrimitive() | BoolPrimitive(_) => NullPrimitive()
              case r => throw new EvalTypeException("Invalid Right Hand Side (Expected Bool)", e, r)
            }
          case r => throw new EvalTypeException("Invalid Left Hand Side (Expected Bool)", e, r)

        }

      // Special case And/Or arithmetic to enable shortcutting
      case Arithmetic(Arith.Or, lhs, rhs) =>
        eval(lhs, bindings) match {
          case BoolPrimitive(true) => BoolPrimitive(true)
          case BoolPrimitive(false) => eval(rhs, bindings)
          case NullPrimitive() => 
            eval(rhs, bindings) match {
              case BoolPrimitive(true) => BoolPrimitive(true)
              case NullPrimitive() | BoolPrimitive(_) => NullPrimitive()
              case r => throw new EvalTypeException("Invalid Right Hand Side (Expected Bool)", e, r)
            }
          case r => throw new EvalTypeException("Invalid Left Hand Side (Expected Bool)", e, r)
        }

      case Arithmetic(op, lhs, rhs) =>
        Eval.applyArith(op, eval(lhs, bindings), eval(rhs, bindings))
      case Comparison(op, lhs, rhs) =>
        Eval.applyCmp(op, eval(lhs, bindings), eval(rhs, bindings))
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
      case Function(name, args) => 
        applyFunction(name.toUpperCase, args.map { eval(_, bindings) })
    }
  }

  def applyFunction(name: String, args: Seq[PrimitiveValue]): PrimitiveValue =
  {
    functions.flatMap { _.getOption(name) } match {
      case Some(NativeFunction(_, op, _, _)) => 
        op(args)
      case Some(ExpressionFunction(_, argNames, expr)) => 
        eval(expr, argNames.zip(args).toMap)
      case Some(FoldFunction(_, expr)) =>
        args.tail.foldLeft[PrimitiveValue](args.head) { case (curr, next) =>
          eval(expr, Map("CURR" -> curr, "NEXT" -> next))
        }
      case None => 
        throw new RAException(s"Function $name(${args.mkString(",")}) is undefined")
    }
  }

  def sampleExpression(exp: Expression): (Double, collection.mutable.Map[Double, Int]) = {
    var sum  = 0.0
    val samples = collection.mutable.Map[Double, Int]()
    for( i <- 0 until SAMPLE_COUNT) {
      val bindings = Map[String, IntPrimitive]("__SEED" -> IntPrimitive(i+1))
      val sample =
        try {
          eval(exp, bindings).asDouble
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


}

object Eval 
{

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
      case _ => e.recur(inline(_, bindings))
    }
  }
  
  /**
   * Perform arithmetic on two primitive values.
   */
  def applyArith(op: Arith.Op, 
            a: PrimitiveValue, b: PrimitiveValue
  ): PrimitiveValue = {
    (op, Typechecker.escalate(
      a.getType, b.getType, "Evaluate Arithmetic", Arithmetic(op, a, b)
    )) match { 
      case (Arith.Add, TInt()) => 
        IntPrimitive(a.asLong + b.asLong)
      case (Arith.Add, TFloat()) => 
        FloatPrimitive(a.asDouble + b.asDouble)
      case (Arith.Sub, TInt()) => 
        IntPrimitive(a.asLong - b.asLong)
      case (Arith.Sub, TFloat()) => 
        FloatPrimitive(a.asDouble - b.asDouble)
      case (Arith.Mult, TInt()) => 
        IntPrimitive(a.asLong * b.asLong)
      case (Arith.Mult, TFloat()) => 
        FloatPrimitive(a.asDouble * b.asDouble)
      case (Arith.Div, (TInt()|TInt())) => 
        IntPrimitive(a.asLong / b.asLong)
      case (Arith.Div, (TFloat()|TInt())) => 
        FloatPrimitive(a.asDouble / b.asDouble)
      case (Arith.BitAnd, TInt()) =>
        IntPrimitive(a.asLong & b.asLong)
      case (Arith.BitOr, TInt()) =>
        IntPrimitive(a.asLong | b.asLong)
      case (Arith.ShiftLeft, TInt()) =>
        IntPrimitive(a.asLong << b.asLong)
      case (Arith.ShiftRight, TInt()) =>
        IntPrimitive(a.asLong >> b.asLong)
      case (Arith.And, TBool()) =>
        BoolPrimitive(a.asBool && b.asBool)
      case (Arith.Or, TBool()) =>
        BoolPrimitive(a.asBool || b.asBool)

      case (Arith.Add, TInterval()) =>
	val p = a.asInterval.plus(b.asInterval);
	IntervalPrimitive(p.getYears,p.getMonths,p.getWeeks,p.getDays,p.getHours,p.getMinutes,p.getSeconds,p.getMillis)


      case (Arith.Sub, TInterval()) =>
	(a.getType, b.getType) match {
		case (TInterval(),TInterval()) =>
			val p = a.asInterval.minus(b.asInterval);
			IntervalPrimitive(p.getYears,p.getMonths,p.getWeeks,p.getDays,p.getHours,p.getMinutes,p.getSeconds,p.getMillis)

		case (TDate() | TTimestamp(),TDate() | TTimestamp()) =>
			val p = new org.joda.time.Period(b.asDateTime, a.asDateTime)
			IntervalPrimitive(p.getYears,p.getMonths,p.getWeeks,p.getDays,p.getHours,p.getMinutes,p.getSeconds,p.getMillis)
		case (_, _) =>
			throw new RAException(s"Invalid Arithmetic $a $op $b")
	}


      case (Arith.Add, TTimestamp()) =>
	val d = a.asDateTime.plus(b.asInterval)
	TimestampPrimitive(d.getYear,d.getMonthOfYear,d.getDayOfMonth,d.getHourOfDay,d.getMinuteOfHour,d.getSecondOfMinute,d. getMillisOfSecond)
      case (Arith.Sub, TTimestamp()) =>
	val d = a.asDateTime.minus(b.asInterval)
	TimestampPrimitive(d.getYear,d.getMonthOfYear,d.getDayOfMonth,d.getHourOfDay,d.getMinuteOfHour,d.getSecondOfMinute,d. getMillisOfSecond)	

      case (_, _) => 
        throw new RAException(s"Invalid Arithmetic $a $op $b")
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
            case TInt() => BoolPrimitive(a.asLong > b.asLong)
            case TFloat() => BoolPrimitive(a.asDouble > b.asDouble)
            case TDate() =>
              BoolPrimitive(
                a.asInstanceOf[DatePrimitive].
                 compare(b.asInstanceOf[DatePrimitive])<0
              )
            case _ => throw new RAException("Invalid Comparison $a $op $b")
          }
        case Cmp.Gte => 
          Typechecker.escalate(a.getType, b.getType, "Eval", Comparison(op, a, b)) match {
            case TInt() => BoolPrimitive(a.asLong >= b.asLong)
            case TFloat() => BoolPrimitive(a.asDouble >= b.asDouble)
            case TDate() =>
              BoolPrimitive(
                a.asInstanceOf[DatePrimitive].
                 compare(b.asInstanceOf[DatePrimitive])<=0
              )
            case _ => throw new RAException("Invalid Comparison $a $op $b")
          }
        case Cmp.Lt => 
          Typechecker.escalate(a.getType, b.getType, "Eval", Comparison(op, a, b)) match {
            case TInt() => BoolPrimitive(a.asLong < b.asLong)
            case TFloat() => BoolPrimitive(a.asDouble < b.asDouble)
            case TDate() =>
              BoolPrimitive(
                a.asInstanceOf[DatePrimitive].
                 compare(b.asInstanceOf[DatePrimitive])>0
              )
            case _ => throw new RAException("Invalid Comparison $a $op $b")
          }
        case Cmp.Lte => 
          Typechecker.escalate(a.getType, b.getType, "Eval", Comparison(op, a, b)) match {
            case TInt() => BoolPrimitive(a.asLong <= b.asLong)
            case TFloat() => BoolPrimitive(a.asDouble <= b.asDouble)
            case TDate() =>
              BoolPrimitive(
                a.asInstanceOf[DatePrimitive].
                 compare(b.asInstanceOf[DatePrimitive])>=0
              )
            case _ => throw new RAException("Invalid Comparison $a $op $b")
          }
      }
    }
  }
}
