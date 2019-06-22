package mimir.ctables

import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir.Database
import mimir.algebra._
import mimir.models._
import mimir.exec.mode.UnannotatedBestGuess

class ReasonSet(val model: Model, val idx: Int, val argLookup: Option[(Operator, Seq[Expression], Seq[Expression])], val toReason: ((Seq[PrimitiveValue], Seq[PrimitiveValue]) => Reason))
{
  def isEmpty(db: Database): Boolean =
  {
    argLookup match {
      case Some((query, argExprs, hintExprs)) => 
        db.query(
          query.count( alias = "COUNT" ), 
          UnannotatedBestGuess
        ) { _.next.tuple(0).asLong <= 0 }
      case None => 
        false
    }    
  }
  def size(db: Database): Long =
  {
    argLookup match {
      case Some((query, argExprs, hintExprs)) => 
        db.query(
          query
            .map( argExprs.zipWithIndex.map { arg => ("ARG_"+arg._2 -> arg._1) }:_* )
            .distinct
            .count( alias = "COUNT" ),
          UnannotatedBestGuess
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

        val argCols = argExprs.zipWithIndex.map   { case (arg,idx) => ProjectArg(ID("ARG_"+idx), arg) }
        val hintCols = hintExprs.zipWithIndex.map { case (arg,idx) => ProjectArg(ID("HINT_"+idx), arg) }

        val projectedQuery =
          Project(argCols ++ hintCols, limitedQuery)

        db.query(projectedQuery, UnannotatedBestGuess) { 
          _.toIndexedSeq.map { _.tuple.splitAt(argExprs.size) } 
        }
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
    allArgs(db).map { case (args, hints) => toReason(args, hints) }
  def take(db: Database, count: Int): Iterable[Reason] = 
    takeArgs(db, count).map { case (args, hints) => toReason(args, hints) }
  def take(db: Database, count: Int, offset:Int): Iterable[Reason] = 
    takeArgs(db, count, offset).map { case (args, hints) => toReason(args, hints) }

  
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
  extends LazyLogging
{
  def make(uncertain: UncertaintyCausingExpression, db: Database, input: Operator): ReasonSet =
  {
    uncertain match {
      case VGTerm(name, idx, argExprs, hintExprs) => {
        logger.debug(s"Make ReasonSet for VGTerm $name from\n$input")
        val model = db.models.get(name)
        return new ReasonSet(
          model,
          idx,
          ( if(argExprs.isEmpty) { None }
            else { Some(input, argExprs, hintExprs) } 
          ),
          (args, hints) => new ModelReason(model, idx, args, hints)
        )
      }
      case DataWarning(name, valueExpr, messageExpr, keyExprs, idx) => {
        logger.debug(s"Make ReasonSet for Warning $name:$idx from\n$input")
        val model = db.models.get(name)
        return new ReasonSet(
          model,
          idx,
          Some(input, keyExprs, Seq(valueExpr, messageExpr)),
          (keys, valueAndMessage) => new DataWarningReason(model, idx, valueAndMessage(0), valueAndMessage(1).asString, keys)
        )
      }

    }
  }
}