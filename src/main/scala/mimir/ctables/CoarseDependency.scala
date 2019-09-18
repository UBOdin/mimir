package mimir.ctables

import play.api.libs.json._
import mimir.algebra.ID

case class CoarseDependency(schema: ID, table: ID)

object CoarseDependency {
  implicit val format: Format[CoarseDependency] = Json.format
}