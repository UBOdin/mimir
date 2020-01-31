package mimir.ctables

import com.typesafe.scalalogging.slf4j.LazyLogging
import play.api.libs.json._
import play.api.libs.functional.syntax._

import mimir.Database
import mimir.algebra._
import mimir.exec.mode.UnannotatedBestGuess
import mimir.serialization.AlgebraJson._

sealed trait ArgLookup

case class MultipleArgLookup(
  val query: Operator,
  val key: Seq[Expression],
  val message: Expression
) extends ArgLookup
case class SingleArgLookup(
  val message: String
) extends ArgLookup

object ArgLookup
{
  implicit val multipleFormat: Format[MultipleArgLookup] = Json.format
  implicit val simgleFormat: Format[SingleArgLookup] = Json.format

  implicit val format = Format[ArgLookup](
    (
      JsPath.read[MultipleArgLookup].asInstanceOf[Reads[ArgLookup]] or 
      JsPath.read[SingleArgLookup].asInstanceOf[Reads[ArgLookup]]
    ),(
      new Writes[ArgLookup]{ def writes(a: ArgLookup) = 
        a match {
          case single: SingleArgLookup => Json.toJson(single)
          case multiple: MultipleArgLookup => Json.toJson(multiple)
        }
      }
    )
  )

}


case class ReasonSet(
  val lens: ID,
  val argLookup: ArgLookup
)
  extends LazyLogging
{
  def isEmpty(db: Database): Boolean =
  {
    argLookup match {
      case MultipleArgLookup(query, argExprs, hintExprs) => 
        db.query(
          query.count( alias = "COUNT" ), 
          UnannotatedBestGuess
        ) { _.next.tuple(0).asLong <= 0 }
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
      case SingleArgLookup(_) => Seq()
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

  def toReason(db: Database, key: Seq[PrimitiveValue], message: String): Reason =
    Reason(
      lens,
      key,
      message,
      db.lenses.isAcknowledged(lens, key)
    )

  def allArgs(db: Database): Iterable[(Seq[PrimitiveValue], String)] =
    allArgs(db, None)
  def takeArgs(db: Database, count: Int): Iterable[(Seq[PrimitiveValue], String)] = 
    allArgs(db, Some(count))
  def takeArgs(db: Database, count: Int, offset: Int): Iterable[(Seq[PrimitiveValue], String)] = 
    allArgs(db, Some(count), Some(offset))

  def all(db: Database): Iterable[Reason] = 
    allArgs(db).map { case (key, message) => toReason(db, key, message) }
  def take(db: Database, count: Int): Iterable[Reason] = 
    takeArgs(db, count).map { case (key, message) => toReason(db, key, message) }
  def take(db: Database, count: Int, offset:Int): Iterable[Reason] = 
    takeArgs(db, count, offset).map { case (key, message) => toReason(db, key, message) }

  
  override def toString: String =
  {
    val lookupString =
      argLookup match {
        case MultipleArgLookup(query, key, message) => "[" + key.mkString(", ") + "][" + message.toString + "] <- \n" + query.toString("   ")
        case SingleArgLookup(messages) => messages.map { "["+_+"]" }.mkString(", ")
      }
    s"$lens${lookupString}"
  }

  def summarize(
    db: Database,
    maxSize: Int = 3
  ): Seq[Reason] =
  {
    val sample = take(db, maxSize+1).toSeq
    if(sample.size > maxSize){
      logger.trace("   -> Too many explanations to fit in one group")
      Seq(
        Reason(
          sample.head.lens, 
          sample.head.key, 
          s"${size(db)} reasons like ${sample.head.message}",
          false
        )
      )
    } else {
      logger.trace(s"   -> Only ${sample.size} explanations")
      sample
    }    
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
          if(keyExprs.isEmpty 
              && ExpressionUtils.getColumns(messageExpr).isEmpty)
                { SingleArgLookup(db.interpreter.evalString(messageExpr)) }
          else  { MultipleArgLookup(input, keyExprs, messageExpr) }
        )
      }

    }
  }

  implicit val format: Format[ReasonSet] = Json.format
}
