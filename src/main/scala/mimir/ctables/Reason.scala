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

  def equals(r: Reason): Boolean = 
    model.name.equals(r.model.name) && 
      (idx == r.idx) && 
      (args.equals(r.args))

  override def hashCode: Int = 
    model.hashCode * idx * args.map(_.hashCode).sum
}

class ReasonSet(model: Model, idx: Int, argLookup: Set[Operator])

object ReasonSet
{
  def make(v:VGTerm, input: Operator): ReasonSet =
  {
    if(v.args.isEmpty){ return new ReasonSet(v.model, v.idx, Set()); }

    val args =
      v.args.zipWithIndex.map { case (expr, i) => ProjectArg("ARG_"+i, expr) }

    return new ReasonSet(
      v.model,
      v.idx,
      Set(Project(args, input))
    );
  }
}