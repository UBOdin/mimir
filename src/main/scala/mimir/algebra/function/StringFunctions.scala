package mimir.algebra.function;

import mimir.algebra._

object StringFunctions
{

  def register(fr: FunctionRegistry)
  {
    fr.register(ID("concat"), 
      (params: Seq[PrimitiveValue]) => 
        StringPrimitive(
            params.map { 
              case NullPrimitive() => "[null]"
              case x => x.asString
            }.mkString
        ),
      (x: Seq[Type]) => TString()
    )

    fr.register(ID("printf"), 
      (params: Seq[PrimitiveValue]) => ???,
      (x: Seq[Type]) => TString()
    )
  }

}