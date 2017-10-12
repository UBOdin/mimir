package mimir.algebra.function;

import mimir.algebra._

object StringFunctions
{

  def register(fr: FunctionRegistry)
  {
    fr.register("CONCAT", 
      (params: Seq[PrimitiveValue]) => StringPrimitive(params.map( _.asString ).mkString),
      (x: Seq[Type]) => TString()
    )

    fr.register("PRINTF", 
      (params: Seq[PrimitiveValue]) => ???,
      (x: Seq[Type]) => TString()
    )

    fr.register("RE_EXTRACT",
      (params: Seq[PrimitiveValue]) => {
        (params(0).asString.r findFirstIn params(1).asString)
          .map { StringPrimitive(_) }
          .getOrElse { NullPrimitive() }
      },
      { 
        case Seq(TString(), _) => TString()
        case _ => throw new RAException("RE_EXTRACT(needle, haystack)")
      }
    )
  }
}