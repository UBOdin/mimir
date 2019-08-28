package mimir.ctables

import java.sql.SQLException
import play.api.libs.json._

import mimir.Database
import mimir.algebra._
import mimir.models._
import mimir.util.JSONBuilder
import mimir.serialization.AlgebraJson._

case class AffectedRow(schema: ID, table: ID, row: RowIdPrimitive)

object AffectedRow
{
  implicit val format: Format[AffectedRow] = Json.format
}


abstract class Reason
{
  def toString: String

  def lens: ID
  def key: Seq[PrimitiveValue]

  def message: String
  def repair: Repair
  def acknowledged: Boolean
  def affectedRows: Seq[AffectedRow]

  def acknowledge(db: Database)
  {
    if(key.isEmpty) { 
      db.lenses.acknowledgeAll(lens)
    } else {
      db.lenses.acknowledge(lens, key)
    }
  }

  def toJSON: String =
    JSONBuilder.dict(Map(
      "english" -> JSONBuilder.string(message),
      "source"  -> JSONBuilder.string(lens.id),
      "args"    -> JSONBuilder.list( key.map( x => JSONBuilder.string(x.toString)).toSeq ),
      "repair"  -> repair.toJSON,
      //TODO:  this is a hack to check if the args
      "affected"-> Json.toJson(affectedRows),
      "confirmed" -> JSONBuilder.boolean(acknowledged) 
    ))
}

class SimpleCaveatReason(
  val lens: ID,
  val key: Seq[PrimitiveValue],
  val message: String,
  affectedRow: Option[AffectedRow],
  val acknowledged: Boolean
)
  extends Reason
{
  override def toString = s"$message {{ $lens[${key.mkString(", ")}] }}"

  def repair: Repair = SimpleCaveatRepair
  def affectedRows = affectedRow.toSeq
}

class MultiReason(
  db: Database, 
  reasons: ReasonSet
)
  extends Reason
{
  var (firstUnackedReason, unackedOffset) = 
    reasons.all(db)
           .zipWithIndex
           .find { !_._1.acknowledged }
           .getOrElse { 
             reasons.take(db, 1)
                    .headOption
                    .map { (_, 0) }
                    .getOrElse { throw new SQLException("Creating MultiReason without causes") }
           }

  def key: Seq[PrimitiveValue] = 
    Option(firstUnackedReason).map { _.key }.getOrElse { Seq() }

  def affectedRows = 
    reasons.take(db, 10)
           .flatMap(_.affectedRows)
           .toSeq
  def lens = firstUnackedReason.lens

  override def toString: String = message//reasons.toString
  def message: String =
    s"${reasons.size(db)} reasons like ${unackedOffset + 1}: ${firstUnackedReason.message}"
  def repair: Repair =
    Option(firstUnackedReason)
      .map { _.repair }
      .getOrElse { EmptyReasonSetRepair }
  def acknowledged = 
    firstUnackedReason.acknowledged
}
