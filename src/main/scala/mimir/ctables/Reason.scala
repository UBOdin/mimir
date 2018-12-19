package mimir.ctables

import mimir.Database
import mimir.algebra._
import mimir.models._
import mimir.util.JSONBuilder

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
      "repair"  -> repair.toJSON,
      //TODO:  this is a hack to check if the args
      "rowidarg"-> JSONBuilder.int(model.argTypes(idx).zipWithIndex.foldLeft(-1)((init, curr) => curr._1 match {
        case TRowId() => curr._2
        case _ => init
      })) 
    ))
    
  def toJSONWithFeedback : String = {
      val argString = 
          if(!this.args.isEmpty){
            " (" + this.args.mkString(",") + ")"
          } else { "" }
      val feedback =  if(!this.confirmed){
          Map(("repair_with", s"`FEEDBACK ${this.model.name} ${this.idx}$argString IS ${ this.repair.exampleString }`"),
          ("confirm_with", s"`FEEDBACK ${this.model.name} ${this.idx}$argString IS ${ this.guess }`"))
        } else {
          Map(("ammend_with", s"`FEEDBACK ${this.model.name} ${this.idx}$argString IS ${ this.repair.exampleString }`"))
        } 
      JSONBuilder.dict(Map(
        "english" -> reason,
        "source"  -> model.name,
        "varid"   -> idx,
        "args"    -> args,
        "repair"  -> repair.toJSON,
        "feedback"-> feedback
      ))
    }
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

class DataWarningReason(
  val model: Model,
  val value: PrimitiveValue,
  val message: String,
  val key: Seq[PrimitiveValue]
)
  extends Reason
{
  override def toString = s" {{ $model[${key.mkString(", ")}] }}"

  def idx: Int = 0
  def args: Seq[PrimitiveValue] = key
  def hints: Seq[PrimitiveValue] = Seq()

  def reason: String = message
  def repair: Repair = DataWarningRepair
  override def guess: PrimitiveValue = value
}

class MultiReason(db: Database, reasons: ReasonSet)
  extends Reason
{
  var ackedOffset = getAckedOffset() 
  def getAckedOffset() : Int = {
    for(i <- 0 to (reasons.size(db).toInt - 1)){
      val taken = reasons.take(db, 1, i).head
      if(!taken.model.isAcknowledged(taken.idx, taken.args))
        return i
    }
    0
  }
  def model = reasons.model
  def idx: Int = reasons.idx
  def args = 
    reasons.takeArgs(db, 1, ackedOffset).head._1
  def hints = 
    reasons.takeArgs(db, 1, ackedOffset).head._2

  override def toString: String = reason//reasons.toString
  def reason: String =
    s"${reasons.size(db)} reasons like ${ackedOffset + 1}: ${reasons.take(db, 1, ackedOffset).head.reason}"
  def repair: Repair =
    reasons.take(db, 1, ackedOffset).head.repair
}
