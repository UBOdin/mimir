package mimir.ctables

import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir.Database
import mimir.algebra._
import mimir.models._
import mimir.exec.mode.UnannotatedBestGuess

sealed trait ArgLookup

case class MultipleArgLookup(
  val query: Operator,
  val key: Seq[Expression],
  val message: Expression
) extends ArgLookup
case class SingleArgLookup(
  val message: Seq[String]
) extends ArgLookup


class ReasonSet(
  val lens: ID,
  val argLookup: ArgLookup,
  val toReason: ((Seq[PrimitiveValue], String) => Reason)
)
{
  def isEmpty(db: Database): Boolean =
  {
    argLookup match {
      case MultipleArgLookup(query, argExprs, hintExprs) => 
        db.query(
          query.count( alias = "COUNT" ), 
          UnannotatedBestGuess
        ) { _.next.tuple(0).asLong <= 0 }
      case SingleArgLookup(Seq()) => true
      case SingleArgLookup(_) => false
    }    
  }
  def size(db: Database): Long =
  {
    argLookup match {
      case MultipleArgLookup(query, argExprs, hintExprs) => 
        db.query(
          query
            .map( argExprs.zipWithIndex.map { arg => ("ARG_"+arg._2 -> arg._1) }:_* )
            .distinct
            .count( alias = "COUNT" ),
          UnannotatedBestGuess
        ) { _.next.tuple(0).asLong }
      case SingleArgLookup(messages) => messages.length
    }
  }
  def allArgs(db: Database, limit: Option[Int] ): Iterable[(Seq[PrimitiveValue], String)] = allArgs(db, limit, None)
  def allArgs(db: Database, limit: Option[Int], offset: Option[Int]): Iterable[(Seq[PrimitiveValue], String)] =
  {
    argLookup match {
      case SingleArgLookup(messages) => messages.map { (Seq(), _) }
      case MultipleArgLookup(baseQuery, argExprs, messageExpr) => {

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
        val messageCol = ProjectArg(ID("MESSAGE"), messageExpr)

        val projectedQuery =
          Project(messageCol +: argCols, limitedQuery)

        db.query(projectedQuery, UnannotatedBestGuess) { 
          _.toIndexedSeq.map { row => (row.tuple.tail, row(0).asString) } 
        }
      }
    }
  }

  def allArgs(db: Database): Iterable[(Seq[PrimitiveValue], String)] =
    allArgs(db, None)
  def takeArgs(db: Database, count: Int): Iterable[(Seq[PrimitiveValue], String)] = 
    allArgs(db, Some(count))
  def takeArgs(db: Database, count: Int, offset: Int): Iterable[(Seq[PrimitiveValue], String)] = 
    allArgs(db, Some(count), Some(offset))

  def all(db: Database): Iterable[Reason] = 
    allArgs(db).map { case (args, message) => toReason(args, message) }
  def take(db: Database, count: Int): Iterable[Reason] = 
    takeArgs(db, count).map { case (args, message) => toReason(args, message) }
  def take(db: Database, count: Int, offset:Int): Iterable[Reason] = 
    takeArgs(db, count, offset).map { case (args, message) => toReason(args, message) }

  
  override def toString: String =
  {
    val lookupString =
      argLookup match {
        case MultipleArgLookup(query, args, message) => "[" + args.mkString(", ") + "][" + message.toString + "] <- \n" + query.toString("   ")
        case SingleArgLookup(messages) => messages.map { "["+_+"]" }.mkString(", ")
      }
    s"$lens${lookupString}"
  }
}

object ReasonSet
  extends LazyLogging
{
  def make(uncertain: UncertaintyCausingExpression, db: Database, input: Operator): ReasonSet =
  {
    uncertain match {
      case Caveat(lens, valueExpr, keyExprs, messageExpr) => {
        logger.debug(s"Make ReasonSet for Caveat $lens from\n$input")
        return new ReasonSet(
          lens,
          Some( ArgLookup(input, keyExprs, messageExpr) ),
          (keys, message) => new SimpleCaveatReason(model, 0, message, keys)
        )
      }

    }
  }
}