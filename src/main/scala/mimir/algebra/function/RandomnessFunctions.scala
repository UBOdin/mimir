package mimir.algebra.function

import mimir.algebra._

object RandomnessFunctions
{

  val prng = new scala.util.Random()

  def register(fr: FunctionRegistry)
  {

    fr.register(ID("random"),
      (args: Seq[PrimitiveValue]) => IntPrimitive(prng.nextLong),
      (types: Seq[Type]) => { TInt() }
    )
    
    fr.register(ID("possion"), 
      {
        case Seq(IntPrimitive(m))   => {
          IntPrimitive(mimir.backend.sqlite.Possion.poisson_helper(m))
        }
        case Seq(FloatPrimitive(m))   => {
          IntPrimitive(mimir.backend.sqlite.Possion.poisson_helper(m))
        }
        case Seq(NullPrimitive())   => NullPrimitive()
        case x => throw new RAException("Non-numeric parameter to possion: '"+x+"'")
      },
      ((args: Seq[Type]) => TInt())
    )

    fr.register(ID("gamma"), 
      {
        case Seq(FloatPrimitive(k), FloatPrimitive(theta))   => {
          FloatPrimitive(mimir.backend.sqlite.Gamma.sampleGamma(k, theta))
        }
        case Seq(NullPrimitive(), FloatPrimitive(r))   => NullPrimitive()
        case Seq(FloatPrimitive(r), NullPrimitive())   => NullPrimitive()
        case Seq(NullPrimitive(), NullPrimitive())   => NullPrimitive()
        case x => throw new RAException("Non-numeric parameter to gamma: '"+x+"'")
      },
      ((args: Seq[Type]) => TFloat())
    )
  }

}