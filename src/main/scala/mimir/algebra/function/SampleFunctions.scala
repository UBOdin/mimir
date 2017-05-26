package mimir.algebra.function;

import mimir.exec.{TupleBundler,WorldBits}
import mimir.algebra._

object SampleFunctions
{
  val prng = new scala.util.Random()

  def register()
  {
    FunctionRegistry.registerNative("RANDOM",
      (args: Seq[PrimitiveValue]) => IntPrimitive(prng.nextLong),
      (types: Seq[Type]) => { TInt() }
    )

    FunctionRegistry.registerNative("BEST_SAMPLE", 
      (args: Seq[PrimitiveValue]) => {
        TupleBundler.mostLikelyValue(
          args.head.asLong,
          args.tail.grouped(2).
            map { arg => (arg(1), arg(0).asDouble) }.toSeq
        ) match {
          case Some(v) => v
          case None => NullPrimitive()
        }
      },
      (types: Seq[Type]) => {
        Typechecker.assertNumeric(types.head, 
          Function("BEST_SAMPLE", types.map(TypePrimitive(_))))
        Typechecker.escalate(
          types.tail.grouped(2).
            map { t => 
              Typechecker.assertNumeric(t(0),
                Function("BEST_SAMPLE", types.map(TypePrimitive(_))))
              t(1)
            }
        )
      }
    )

    FunctionRegistry.registerNative("SAMPLE_CONFIDENCE",
      (args: Seq[PrimitiveValue]) => 
        FloatPrimitive(
          WorldBits.confidence(args(0).asLong, args(0).asLong.toInt)
        ),
      (types: Seq[Type]) => {
        Typechecker.assertNumeric(types(0), 
          Function("SAMPLE_CONFIDENCE", types.map(TypePrimitive(_))))
        Typechecker.assertNumeric(types(1),
          Function("SAMPLE_CONFIDENCE", types.map(TypePrimitive(_))))
        TFloat()
      }
    )
  }

}