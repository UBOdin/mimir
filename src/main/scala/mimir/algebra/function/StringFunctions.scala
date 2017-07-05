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
  }

}