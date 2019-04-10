package mimir.api

import play.api.libs.json._


sealed abstract class Response {
  
}

object Response {
  
}

case class ScalaEvalResponse (
            /* stdout from evaluation of scala code */
                  stdout: String,
            /* stderr from evaluation of scala code */
                  stderr: String
) extends Response

object ScalaEvalResponse {
  implicit val format: Format[ScalaEvalResponse] = Json.format
}


case class LoadResponse (
            /* name of resulting table */
                  name: String
) extends Response

object LoadResponse {
  implicit val format: Format[LoadResponse] = Json.format
}


case class CreateLensResponse (
            /* name of resulting lens */
                  lensName: String,
            /* count of annotations from lens */
                  annotations: Long
) extends Response

object CreateLensResponse {
  implicit val format: Format[CreateLensResponse] = Json.format
}


case class CreateViewResponse (
            /* name of resulting view */
                  lensName: String
) extends Response

object CreateViewResponse {
  implicit val format: Format[CreateViewResponse] = Json.format
}


case class AdaptiveSchemaList (
    adaptiveSchemaTypes: Seq[String]
)

object AdaptiveSchemaList {
  implicit val format: Format[AdaptiveSchemaList] = Json.format
}


case class CreateAdaptiveSchemaResponse (
            /* name of resulting adaptive schema */
                  adaptiveSchemaName: String
)

object CreateAdaptiveSchemaResponse {
  implicit val format: Format[CreateAdaptiveSchemaResponse] = Json.format
}




case class CSVContainer (
                  schema: Seq[Schema],
                  data: Seq[Seq[String]],
                  prov: Seq[String],
                  colTaint: Seq[Seq[Boolean]],
                  rowTaint: Seq[Boolean],
                  reasons: Seq[Seq[Reason]]
)

object CSVContainer {
  implicit val format: Format[CSVContainer] = Json.format
}


case class ExplainReasonsResponse (
                  reasons: Seq[Reason]
)

object ExplainReasonsResponse {
  implicit val format: Format[ExplainReasonsResponse] = Json.format
}


case class ExplainResponse (
                  reasons: Seq[ReasonSet]
)

object ExplainResponse {
  implicit val format: Format[ExplainResponse] = Json.format
}


case class LensList (
    lensTypes: Seq[String]
)

object LensList {
  implicit val format: Format[LensList] = Json.format
}



case class Repair (
            /* name of selector */
                  selector: String
)

object Repair {
  implicit val format: Format[Repair] = Json.format
}


case class SchemaList (
    schema: Seq[Schema]
)

object SchemaList {
  implicit val format: Format[SchemaList] = Json.format
}



