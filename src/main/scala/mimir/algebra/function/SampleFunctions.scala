package mimir.algebra.function;

import mimir.algebra._
import java.sql.SQLException

object SampleFunctions
{

  def register(fr: FunctionRegistry)
  {

    
    fr.register(ID("possion"), 
      {
	      case Seq(IntPrimitive(m))   => {
          IntPrimitive(mimir.exec.sqlite.Possion.poisson_helper(m))
	      }
        case Seq(FloatPrimitive(m))   => {
          IntPrimitive(mimir.exec.sqlite.Possion.poisson_helper(m))
	      }
        case Seq(NullPrimitive())   => NullPrimitive()
	      case x => throw new SQLException("Non-numeric parameter to possion: '"+x+"'")
      },
      ((args: Seq[Type]) => TInt())
		)
		
		fr.register(ID("gamma"), 
      {
	      case Seq(FloatPrimitive(k), FloatPrimitive(theta))   => {
          FloatPrimitive(mimir.exec.sqlite.Gamma.sampleGamma(k, theta))
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