package mimir.algebra.function;

import mimir.algebra._
import mimir.provenance._
import mimir.util._

object TypeFunctions
{

  def register()
  {
    FunctionRegistry.registerNative("MIMIR_MAKE_ROWID", 
      Provenance.joinRowIds(_: Seq[PrimitiveValue]),
      ((args: Seq[Type]) => TRowId())
    )

    FunctionRegistry.registerSet(List("CAST", "MIMIRCAST"), 
      (params: Seq[PrimitiveValue]) => {
        params match {
          case x :: TypePrimitive(t)    :: Nil => Cast(t, x)
          case _ => throw new RAException("Invalid cast: "+params)
        }
      },
      (_) => TAny()
    )

    FunctionRegistry.registerSet(List("DATE", "TO_DATE"), 
      (params: Seq[PrimitiveValue]) => 
          { TextUtils.parseDate(params.head.asString) },
      _ match {
        case _ :: Nil => TDate()
        case _ => throw new RAException("Invalid parameters to DATE()")
      }
    )



  }

}