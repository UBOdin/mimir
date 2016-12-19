package mimir.models

import mimir.algebra._
import mimir.util.JSONBuilder

case class Reason(
  val reason: String,
  val model: String,
  val idx: Int,
  val args: Seq[PrimitiveValue]
){
  override def toString: String = 
    reason+" ("+model+";"+idx+"["+args.mkString(", ")+"])"

  def toJSON: String =
    JSONBuilder.dict(Map(
      "english" -> JSONBuilder.string(reason),
      "source"  -> JSONBuilder.string(model),
      "varid"   -> JSONBuilder.int(idx),
      "args"    -> JSONBuilder.list( args.map( x => JSONBuilder.string(x.toString)).toList )
    ))
}
