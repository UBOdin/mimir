package mimir.ctables

import mimir.Database
import mimir.algebra._
import mimir.models._
import mimir.util.JSONBuilder

object Reason
{
  def make(term: VGTerm, v: Seq[PrimitiveValue], h: Seq[PrimitiveValue]): Reason =
    new ModelReason(term.model, term.idx, v, h)
}

abstract class Reason
{
  def toString: String

  def model: Model
  def idx: Int
  def args: Seq[PrimitiveValue]
  def hints: Seq[PrimitiveValue]

  def reason: String
  def repair: Repair
  def guess: PrimitiveValue = model.bestGuess(idx, args, hints)
  def confirmed: Boolean = model.isAcknowledged(idx, args)

  def toJSON: String =
    JSONBuilder.dict(Map(
      "english" -> JSONBuilder.string(reason),
      "source"  -> JSONBuilder.string(model.name),
      "varid"   -> JSONBuilder.int(idx),
      "args"    -> JSONBuilder.list( args.map( x => JSONBuilder.string(x.toString)).toList ),
      "repair"  -> repair.toJSON
    ))
}

class ModelReason(
  val model: Model,
  val idx: Int,
  val args: Seq[PrimitiveValue],
  val hints: Seq[PrimitiveValue]
)
  extends Reason
{
  override def toString: String = 
    reason+" {{"+model+";"+idx+"["+args.mkString(", ")+"]}}"

  def reason: String =
    model.reason(idx, args, hints)

  def repair: Repair = 
    Repair.makeRepair(model, idx, args, hints)

  def equals(r: Reason): Boolean = 
    model.name.equals(r.model.name) && 
      (idx == r.idx) && 
      (args.equals(r.args))

  override def hashCode: Int = 
    model.hashCode * idx * args.map(_.hashCode).sum
}

class MultiReason(db: Database, reasons: ReasonSet)
  extends Reason
{
  def model = reasons.model
  def idx: Int = reasons.idx
  def args = 
    reasons.takeArgs(db, 1).head._1
  def hints = 
    reasons.takeArgs(db, 1).head._2

  override def toString: String = reasons.toString
  def reason: String =
    s"${reasons.size(db)} reasons like ${reasons.take(db, 1).head.reason}"
  def repair: Repair =
    reasons.take(db, 1).head.repair
}
