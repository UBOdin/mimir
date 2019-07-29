package mimir.algebra.function;

import mimir.exec.mode.{TupleBundle,WorldBits}
import mimir.algebra._
import java.sql.SQLException

object SampleFunctions
{

  def register(fr: FunctionRegistry)
  {

    fr.register(ID("best_sample"), 
      (args: Seq[PrimitiveValue]) => {
        TupleBundle.mostLikelyValue(
          args.head.asLong,
          args.tail.grouped(2).
            map { arg => (arg(1), arg(0).asDouble) }.toSeq
        ) match {
          case Some(v) => v
          case None => NullPrimitive()
        }
      },
      (types: Seq[Type]) => {
        val debugExpr = Function("best_sample", types.map(TypePrimitive(_)):_*)

        Typechecker.assertNumeric(types.head, debugExpr)
        Typechecker.assertLeastUpperBound(
          types.tail.grouped(2).
            map { t => 
              Typechecker.assertNumeric(t(0),debugExpr)
              t(1)
            },
          "BEST_SAMPLE",
          debugExpr
        )
      }
    )

    fr.register(ID("sample_confidence"),
      (args: Seq[PrimitiveValue]) => 
        FloatPrimitive(
          WorldBits.confidence(args(0).asLong, args(0).asLong.toInt)
        ),
      (types: Seq[Type]) => {
        Typechecker.assertNumeric(types(0), 
          Function("sample_confidence", types.map(TypePrimitive(_)):_*))
        Typechecker.assertNumeric(types(1),
          Function("sample_confidence", types.map(TypePrimitive(_)):_*))
        TFloat()
      }
    )
    
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