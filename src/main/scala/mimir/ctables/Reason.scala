package mimir.ctables

import mimir.algebra._
import mimir.models._
import mimir.util.JSONBuilder

class Reason(
  val model: Model,
  val idx: Int,
  val args: Seq[PrimitiveValue]
){
  override def toString: String = 
    reason+" ("+model+";"+idx+"["+args.mkString(", ")+"])"

  def reason: String =
    model.reason(idx, args)

  def repair: Repair = 
    Repair.makeRepair(model, idx, args)

  def toJSON: String =
    JSONBuilder.dict(Map(
      "english" -> JSONBuilder.string(reason),
      "source"  -> JSONBuilder.string(model.name),
      "varid"   -> JSONBuilder.int(idx),
      "args"    -> JSONBuilder.list( args.map( x => JSONBuilder.string(x.toString)).toList ),
      "repair"  -> repair.toJSON
    ))
}

case class ReasonSet(model: String, idx: Int, args: Operator)