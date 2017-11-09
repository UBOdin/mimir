package mimir.algebra.function;

import mimir.algebra._

object NumericFunctions
{

  def register(fr: FunctionRegistry)
  {
    fr.register("ABSOLUTE", 
      {
        case Seq(IntPrimitive(i))   => if(i < 0){ IntPrimitive(-i) } else { IntPrimitive(i) }
        case Seq(FloatPrimitive(f)) => if(f < 0){ FloatPrimitive(-f) } else { FloatPrimitive(f) }
        case Seq(NullPrimitive())   => NullPrimitive()
        case x => throw new RAException("Non-numeric parameter to absolute: '"+x+"'")
      },
      (x: Seq[Type]) => Typechecker.assertNumeric(x(0), Function("ABSOLUTE", List()))
    )

    fr.register("SQRT",
      {
        case Seq(n:NumericPrimitive) => FloatPrimitive(Math.sqrt(n.asDouble))
      },
      (x: Seq[Type]) => Typechecker.assertNumeric(x(0), Function("SQRT", List()))
    )

    fr.register("BITWISE_AND", 
      (x) => IntPrimitive(x(0).asLong & x(1).asLong), 
      (_) => TInt()
    )

    fr.register("BITWISE_OR", 
      (x) => IntPrimitive(x(0).asLong | x(1).asLong), 
      (_) => TInt()
    )
    
    fr.register("AVG",(_) => ???, (_) => TInt())
    fr.register("STDDEV",(_) => ???, (_) => TFloat())
    fr.register("min",
        {
          case ints:Seq[IntPrimitive] => IntPrimitive(ints.foldLeft(ints.head.v)( (init, intval) => Math.min(init, intval.v)))
        }, (_) => TInt())
    fr.register("max",{
          case ints:Seq[IntPrimitive] => IntPrimitive(ints.foldLeft(ints.head.v)( (init, intval) => Math.max(init, intval.v)))
        }, (_) => TInt())
    
    fr.register("ROUND",
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

    fr.register("ABS", 
      { args => Eval.applyAbs(args(0)) },
      {
        case x @ Seq(TInt() | TFloat() | TInterval()) => x(0)
        case x => throw new RAException(s"Invalid ABS($x)")
      }
    )
  }

}