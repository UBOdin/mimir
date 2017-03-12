package mimir.ctables

import mimir.Database
import mimir.algebra._
import mimir.models._

class ReasonSet(val model: Model, val idx: Int, argLookup: Option[(Operator, Seq[Expression], Seq[Expression])])
{
  def size(db: Database): Long =
  {
    argLookup match {
      case Some((query, argExprs, hintExprs)) => 
        db.query(
          Aggregate(List(), List(AggFunction("COUNT", false, List(), "COUNT")), 
            OperatorUtils.makeDistinct(
              Project(
                argExprs.zipWithIndex.map { arg => ProjectArg("ARG_"+arg._2, arg._1) },
                query
              )
            )
          )
        ).allRows.head(0).asLong
      case None => 
        1
    }
  }
  def allArgs(db: Database, limit: Option[Int]): Iterable[(Seq[PrimitiveValue], Seq[PrimitiveValue])] =
  {
    argLookup match {
      case None => Seq((Seq[PrimitiveValue](), Seq[PrimitiveValue]()))
      case Some((baseQuery, argExprs, hintExprs)) => {

        val limitedQuery = 
          limit match {
            case None => baseQuery
            case Some(rowCount) => Limit(0, Some(rowCount.toLong), baseQuery)
          }

        val argCols = argExprs.zipWithIndex.map { arg => ProjectArg("ARG_"+arg._2, arg._1) }
        val hintCols = hintExprs.zipWithIndex.map { arg => ProjectArg("HINT_"+arg._2, arg._1) }

        val projectedQuery =
          Project(argCols ++ hintCols, limitedQuery)

        db.query(projectedQuery).allRows.map( _.splitAt(argExprs.size) )
      }
    }
  }

  def allArgs(db: Database): Iterable[(Seq[PrimitiveValue], Seq[PrimitiveValue])] =
    allArgs(db, None)
  def takeArgs(db: Database, count: Int): Iterable[(Seq[PrimitiveValue], Seq[PrimitiveValue])] = 
    allArgs(db, Some(count))

  def all(db: Database): Iterable[Reason] = 
    allArgs(db).map { case (args, hints) => new Reason(model, idx, args, hints) }
  def take(db: Database, count: Int): Iterable[Reason] = 
    takeArgs(db, count).map { case (args, hints) => new Reason(model, idx, args, hints) }
}

object ReasonSet
{
  def make(v:VGTerm, input: Operator): ReasonSet =
  {
    if(v.args.isEmpty){ return new ReasonSet(v.model, v.idx, None); }

    return new ReasonSet(
      v.model,
      v.idx,
      Some((input, v.args, v.hints))
    );
  }
}