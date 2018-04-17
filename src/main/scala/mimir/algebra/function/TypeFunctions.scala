package mimir.algebra.function;

import mimir.algebra._
import mimir.provenance._
import mimir.util._

object TypeFunctions
{

  def joinRowIds(rowids: Seq[PrimitiveValue]): RowIdPrimitive =
  {
    RowIdPrimitive(rowids.map(_.asString).mkString("|"))
  }
  
  def register(fr: FunctionRegistry)
  {
    fr.register("MIMIR_MAKE_ROWID", 
      joinRowIds(_: Seq[PrimitiveValue]),
      ((args: Seq[Type]) => TRowId())
    )

    for(functionName <- Seq("CAST", "MIMIRCAST")){
      fr.register(
        functionName,
        (params: Seq[PrimitiveValue]) => {
          params match {
            case Seq(x, TypePrimitive(t)) => Cast(t, x)
            case _ => throw new RAException("Invalid cast: "+params)
          }
        },
        (_) => TAny()
      )
    }

    for(functionName <- Seq("DATE", "TO_DATE")){
      fr.register(
        functionName,
        (params: Seq[PrimitiveValue]) => 
            { TextUtils.parseDate(params.head.asString) },
        _ match {
          case Seq(_) => TDate()
          case _ => throw new RAException("Invalid parameters to DATE()")
        }
      )
    }

    for(functionName <- Seq("INTERVAL", "TO_INTERVAL")){
      fr.register(
        functionName,
        (params: Seq[PrimitiveValue]) => 
            { TextUtils.parseInterval(params.head.asString) },
        _ match {
          case Seq(_) => TInterval()
          case _ => throw new RAException("Invalid parameters to INTERVAL()")
        }
      )
    }


  }

}
