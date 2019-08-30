package mimir.ctables

import java.sql.SQLException
import play.api.libs.json._

import mimir.Database
import mimir.algebra._
import mimir.models._
import mimir.util.JSONBuilder
import mimir.serialization.AlgebraJson._


case class Reason(
  val lens: ID,
  val key: Seq[PrimitiveValue],
  val message: String,
  val acknowledged: Boolean
)
{
  def acknowledge(db: Database)
  {
    if(key.isEmpty) { 
      db.lenses.acknowledgeAll(lens)
    } else {
      db.lenses.acknowledge(lens, key)
    }
  }
}

object Reason
{
  implicit val format: Format[Reason] = Json.format
}

