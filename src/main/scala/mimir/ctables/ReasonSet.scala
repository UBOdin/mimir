package mimir.ctables

import mimir.Database
import mimir.algebra._
import mimir.models._

class ReasonSet(val model: Model, val idx: Int, val argLookup: Option[(Operator, Seq[Expression], Seq[Expression])])
{
  def size(db: Database): Long =
  {
    argLookup match {
      case Some((query, argExprs, hintExprs)) => 
        db.query(
          query
            .map( argExprs.zipWithIndex.map { arg => ("ARG_"+arg._2 -> arg._1) }:_* )
            .distinct
            .count( alias = "COUNT" )
        ) { _.next.tuple(0).asLong }
      case None => 
        1
    }
  }
  def allArgs(db: Database, limit: Option[Int] ): Iterable[(Seq[PrimitiveValue], Seq[PrimitiveValue])] = allArgs(db, limit, None)
  def allArgs(db: Database, limit: Option[Int], offset: Option[Int]): Iterable[(Seq[PrimitiveValue], Seq[PrimitiveValue])] =
  {
    argLookup match {
      case None => Seq((Seq[PrimitiveValue](), Seq[PrimitiveValue]()))
      case Some((baseQuery, argExprs, hintExprs)) => {

        val limitedQuery = 
          limit match {
            case None => baseQuery
            case Some(rowCount) => {
              val rowOffset = offset match {
                case None => 0
                case Some(rowOffset) => rowOffset
              }
              Limit(rowOffset, Some(rowCount.toLong), baseQuery)
            }
          }

        val argCols = argExprs.zipWithIndex.map { arg => ProjectArg("ARG_"+arg._2, arg._1) }
        val hintCols = hintExprs.zipWithIndex.map { arg => ProjectArg("HINT_"+arg._2, arg._1) }

        val projectedQuery =
          Project(argCols ++ hintCols, limitedQuery)

        db.query(projectedQuery) { _.toIndexedSeq.map { _.tuple.splitAt(argExprs.size) } }
      }
    }
  }

  def allArgs(db: Database): Iterable[(Seq[PrimitiveValue], Seq[PrimitiveValue])] =
    allArgs(db, None)
  def takeArgs(db: Database, count: Int): Iterable[(Seq[PrimitiveValue], Seq[PrimitiveValue])] = 
    allArgs(db, Some(count))
  def takeArgs(db: Database, count: Int, offset: Int): Iterable[(Seq[PrimitiveValue], Seq[PrimitiveValue])] = 
    allArgs(db, Some(count), Some(offset))

  def all(db: Database): Iterable[Reason] = 
    allArgs(db).map { case (args, hints) => new ModelReason(model, idx, args, hints) }
  def take(db: Database, count: Int): Iterable[Reason] = 
    takeArgs(db, count).map { case (args, hints) => new ModelReason(model, idx, args, hints) }
  def take(db: Database, count: Int, offset:Int): Iterable[Reason] = 
    takeArgs(db, count, offset).map { case (args, hints) => new ModelReason(model, idx, args, hints) }

  
  override def toString: String =
  {
    val lookupString =
      argLookup match {
        case Some((query, args, hints)) => "[" + args.mkString(", ") + "][" + hints.mkString(", ") + "] <- \n" + query.toString("   ")
        case None => ""
      }
    s"${model.name};${idx}${lookupString}"
  }
}

object ReasonSet
{
  def make(vgterm: VGTerm, db: Database, input: Operator): ReasonSet =
  {
    if(vgterm.args.isEmpty){
      return new ReasonSet(
        db.models.get(vgterm.name),
        vgterm.idx,
        None
      )
    } else {
      return new ReasonSet(
        db.models.get(vgterm.name),
        vgterm.idx,
        Some(
          input,
          vgterm.args,
          vgterm.hints
        )
      )
    }
  }
}