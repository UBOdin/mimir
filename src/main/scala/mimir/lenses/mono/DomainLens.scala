package mimir.lenses.mono

import java.sql.SQLException
import play.api.libs.json._
import sparsity.Name
import mimir.Database
import mimir.algebra._
import mimir.lenses._
import mimir.util.NameLookup
import mimir.serialization.AlgebraJson._

case class DomainLensColumnConfig(
  rule: Expression,
  mode: ID
)
object DomainLensColumnConfig
{
  implicit val format: Format[DomainLensColumnConfig] = Json.format
}


object DomainLens extends MonoLens
{
  def train(
    db: Database,
    name: ID,
    query: Operator,
    config: JsValue
  ): JsValue = ???
  
  def view(
    db: Database,
    name: ID,
    query: Operator,
    config: JsValue,
    friendlyName: String
  ): Operator = ???
}