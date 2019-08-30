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


case class Repair (
            /* name of selector */
                  selector: String
)

object Repair {
  implicit val format: Format[Repair] = Json.format
}
