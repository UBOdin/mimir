package mimir.algebra.function;

import mimir.algebra._

object NumericFunctions
{

  def register(fr: FunctionRegistry)
  {
    fr.register(ID("absolute"), 
      {
        case Seq(IntPrimitive(i))   => if(i < 0){ IntPrimitive(-i) } else { IntPrimitive(i) }
        case Seq(FloatPrimitive(f)) => if(f < 0){ FloatPrimitive(-f) } else { FloatPrimitive(f) }
        case Seq(NullPrimitive())   => NullPrimitive()
        case x => throw new RAException("Non-numeric parameter to absolute: '"+x+"'")
      },
      (x: Seq[Type]) => Typechecker.assertNumeric(x(0), Function("ABSOLUTE"))
    )

    fr.register(ID("sqrt"),
      {
        case Seq(n:NumericPrimitive) => FloatPrimitive(Math.sqrt(n.asDouble))
      },
      (x: Seq[Type]) => Typechecker.assertNumeric(x(0), Function("SQRT"))
    )

    fr.register(ID("bitwise_and"), 
      (x) => IntPrimitive(x(0).asLong & x(1).asLong), 
      (_) => TInt()
    )

    fr.register(ID("bitwise_or"), 
      (x) => IntPrimitive(x(0).asLong | x(1).asLong), 
      (_) => TInt()
    )
    
    fr.register(ID("avg"),(_) => ???, (_) => TInt())
    fr.register(ID("stddev"),(_) => ???, (_) => TFloat())
    fr.register(ID("min"),
        {
          case ints:Seq[_] => IntPrimitive(ints.foldLeft(ints.head.asInstanceOf[IntPrimitive].v)( (init, intval) => Math.min(init, intval.asInstanceOf[IntPrimitive].v)))
        }, (_) => TInt())
    fr.register(ID("max"),{
          case ints:Seq[_] => IntPrimitive(ints.foldLeft(ints.head.asInstanceOf[IntPrimitive].v)( (init, intval) => Math.max(init, intval.asInstanceOf[IntPrimitive].v)))
        }, (_) => TInt())
    fr.register(ID("min"),
        {
          case ints:Seq[_] => IntPrimitive(ints.foldLeft(ints.head.asInstanceOf[IntPrimitive].v)( (init, intval) => Math.min(init, intval.asInstanceOf[IntPrimitive].v)))
        }, (_) => TInt())
    fr.register(ID("max"),{
          case ints:Seq[_] => IntPrimitive(ints.foldLeft(ints.head.asInstanceOf[IntPrimitive].v)( (init, intval) => Math.max(init, intval.asInstanceOf[IntPrimitive].v)))
        }, (_) => TInt())
    
    fr.register(ID("round"),
      {
        case Seq(FloatPrimitive(number),IntPrimitive(decimalPlaces)) => {
          FloatPrimitive(s"%.${decimalPlaces}f".format(number).toDouble)
        }
        case Seq(FloatPrimitive(number)) => {
          FloatPrimitive(Math.round(number).toDouble)
        }
      },
      (x: Seq[Type]) => TFloat()
    )

    fr.register(ID("abs"), 
      { args => Eval.applyAbs(args(0)) },
      {
        case x @ Seq(TInt() | TFloat() | TInterval()) => x(0)
        case x => throw new RAException(s"Invalid ABS($x)")
      }
    )
  }

}