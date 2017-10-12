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
        re_extract(params(0).asString, params(1).asString)
          .map { StringPrimitive(_) }
          .getOrElse { NullPrimitive() }
      },
      { 
        case Seq(TString(), _) => TString()
        case _ => throw new RAException("RE_EXTRACT(needle, haystack)")
      }
    )
  }

  def re_extract(needle: String, haystack: String): Option[String] =
  {
    (needle.r findFirstMatchIn haystack) match {
      case Some(m) => 
        if(m.groupCount > 0){ Some(m.group(1)) }
        else { Some(m.matched) }
      case None => 
        None
    }

  }
}