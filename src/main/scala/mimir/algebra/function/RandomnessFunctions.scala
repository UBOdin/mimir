package mimir.algebra.function


object RandomnessFunctions
{

  val prng = new scala.util.Random()

  def register()
  {

    FunctionRegistry.registerNative("RANDOM",
      (args: Seq[PrimitiveValue]) => IntPrimitive(prng.nextLong),
      (types: Seq[Type]) => { TInt() }
    )

    FunctionRegistry.registerNative("POSSION", 
      {
        case Seq(IntPrimitive(m))   => {
          IntPrimitive(mimir.sql.sqlite.Possion.poisson_helper(m))
        }
        case Seq(FloatPrimitive(m))   => {
          IntPrimitive(mimir.sql.sqlite.Possion.poisson_helper(m))
        }
        case Seq(NullPrimitive())   => NullPrimitive()
        case x => throw new SQLException("Non-numeric parameter to possion: '"+x+"'")
      },
      ((args: Seq[Type]) => TInt())
    )

    FunctionRegistry.registerNative("GAMMA", 
      {
        case Seq(FloatPrimitive(k), FloatPrimitive(theta))   => {
          FloatPrimitive(mimir.sql.sqlite.Gamma.sampleGamma(k, theta))
        }
        case Seq(NullPrimitive(), FloatPrimitive(r))   => NullPrimitive()
        case Seq(FloatPrimitive(r), NullPrimitive())   => NullPrimitive()
        case Seq(NullPrimitive(), NullPrimitive())   => NullPrimitive()
        case x => throw new SQLException("Non-numeric parameter to gamma: '"+x+"'")
      },
      ((args: Seq[Type]) => TFloat())
    )
  }

}