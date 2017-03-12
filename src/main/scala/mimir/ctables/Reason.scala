package mimir.ctables

import mimir.Database
import mimir.algebra._
import mimir.models._
import mimir.util.JSONBuilder

class Reason(
  val model: Model,
  val idx: Int,
  val args: Seq[PrimitiveValue],
  val hints: Seq[PrimitiveValue]
){
  override def toString: String = 
    reason+" ("+model+";"+idx+"["+args.mkString(", ")+"])"

  def reason: String =
    model.reason(idx, args, hints)

  def repair: Repair = 
    Repair.makeRepair(model, idx, args, hints)

  def toJSON: String =
    JSONBuilder.dict(Map(
      "english" -> JSONBuilder.string(reason),
      "source"  -> JSONBuilder.string(model.name),
      "varid"   -> JSONBuilder.int(idx),
      "args"    -> JSONBuilder.list( args.map( x => JSONBuilder.string(x.toString)).toList ),
      "repair"  -> repair.toJSON
    ))

  def equals(r: Reason): Boolean = 
    model.name.equals(r.model.name) && 
      (idx == r.idx) && 
      (args.equals(r.args))

  override def hashCode: Int = 
    model.hashCode * idx * args.map(_.hashCode).sum
}

object Reason
{
  def make(term: VGTerm, v: Seq[PrimitiveValue], h: Seq[PrimitiveValue]): Reason =
    new Reason(term.model, term.idx, v, h)
}
