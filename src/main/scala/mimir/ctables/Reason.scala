package mimir.ctables

import mimir.Database
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

class ReasonSet(val model: Model, val idx: Int, argLookup: Option[Operator])
{
  def size(db: Database): Long =
  {
    argLookup match {
      case Some(query) => 
        db.query(
          Aggregate(List(), List(AggFunction("COUNT", false, List(), "COUNT")), 
            OperatorUtils.makeDistinct(query)
          )
        ).allRows.head(0).asLong
      case None => 
        1
    }
  }
  def all(db: Database): Iterable[Reason] = 
  {
    argLookup match {
      case Some(query) =>
        db.query(query).mapRows( row => new Reason(model, idx, row.currentRow) )
      case None => 
        new Some(new Reason(model, idx, List()))
    }
  }
  def take(db: Database, count: Int): Iterable[Reason] = 
  {
    if(count < 1){ return None }
    argLookup match {
      case Some(query) =>
        db.query(
          Limit(0, Some(count), query)
        ).mapRows( row => new Reason(model, idx, row.currentRow) )
      case None => 
        new Some(new Reason(model, idx, List()))
    }
  }
}

object ReasonSet
{
  def make(v:VGTerm, input: Operator): ReasonSet =
  {
    if(v.args.isEmpty){ return new ReasonSet(v.model, v.idx, None); }

    val args =
      v.args.zipWithIndex.map { case (expr, i) => ProjectArg("ARG_"+i, expr) }

    return new ReasonSet(
      v.model,
      v.idx,
      Some(Project(args, input))
    );
  }
}