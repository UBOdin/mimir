package mimir.algebra.function;

import mimir.algebra._

object StringFunctions
{

  def register()
  {
    FunctionRegistry.registerNative("CONCAT", 
      (params: Seq[PrimitiveValue]) => StringPrimitive(params.map( _.asString ).mkString),
      (x: Seq[Type]) => TString()
    )

    FunctionRegistry.registerNative("PRINTF", 
      (params: Seq[PrimitiveValue]) => ???,
      (x: Seq[Type]) => TString()
    )
  }

}