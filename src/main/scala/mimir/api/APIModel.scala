package mimir.api

import play.api.libs.json._

case class Tuple (
            /* name */
                  name: String,
            /* value */
                  value: String
)

object Tuple {
  implicit val format: Format[Tuple] = Json.format
}

case class Schema (
            /* name of the element */
                  name: String,
            /* type name of the element */
                  `type`: String,
            /* base type name of the element */
                  baseType: String
)

object Schema {
  implicit val format: Format[Schema] = Json.format
}

case class Reason (
                  english: String,
                  source: String,
                  varid: Int,
                  args: Seq[String],
                  repair: Repair,
                  feedback: String
)

object Reason {
  implicit val format: Format[Reason] = Json.format
}

case class ReasonSet (
            /* model name */
                  model: String,
            /* idx for vgterm */
                  idx: Long,
            /* lookup string for args and hints */
                  lookup: String
)

object ReasonSet {
  implicit val format: Format[ReasonSet] = Json.format
}