package mimir.algebra.function;

import mimir.algebra._

object NumericFunctions
{

  def register()
  {
    FunctionRegistry.registerNative("ABSOLUTE", 
      {
        case Seq(IntPrimitive(i))   => if(i < 0){ IntPrimitive(-i) } else { IntPrimitive(i) }
        case Seq(FloatPrimitive(f)) => if(f < 0){ FloatPrimitive(-f) } else { FloatPrimitive(f) }
        case Seq(NullPrimitive())   => NullPrimitive()
        case x => throw new RAException("Non-numeric parameter to absolute: '"+x+"'")
      },
      (x: Seq[Type]) => Typechecker.assertNumeric(x(0), Function("ABSOLUTE", List()))
    )

    FunctionRegistry.registerNative("SQRT",
      {
        case Seq(n:NumericPrimitive) => FloatPrimitive(Math.sqrt(n.asDouble))
      },
      (x: Seq[Type]) => Typechecker.assertNumeric(x(0), Function("SQRT", List()))
    )

    FunctionRegistry.registerNative("BITWISE_AND", 
      (x) => IntPrimitive(x(0).asLong & x(1).asLong), 
      (_) => TInt()
    )

    FunctionRegistry.registerNative("BITWISE_OR", 
      (x) => IntPrimitive(x(0).asLong | x(1).asLong), 
      (_) => TInt()
    )
    
    FunctionRegistry.registerNative("AVG",(_) => ???, (_) => TInt())
    FunctionRegistry.registerNative("STDDEV",(_) => ???, (_) => TFloat())

  }

}