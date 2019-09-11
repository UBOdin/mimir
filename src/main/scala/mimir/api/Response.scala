package mimir.api

import play.api.libs.json._
import mimir.serialization.PrimitiveValueJSON._
import mimir.algebra.PrimitiveValue

sealed abstract class Response {
  
}

object Response {
  
}

case class ErrorResponse (
            /* throwable class name */
                  errorType: String,
            /* throwable message */
                  errorMessage: String,
            /* throwable stack trace */
                  stackTrace: String
) extends Response

object ErrorResponse {
  implicit val format: Format[ErrorResponse] = Json.format
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


case class UnloadResponse (
            /* name of resulting table */
                  outputFiles: Seq[String]
) extends Response

object UnloadResponse {
  implicit val format: Format[UnloadResponse] = Json.format
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
                  viewName: String
) extends Response

object CreateViewResponse {
  implicit val format: Format[CreateViewResponse] = Json.format
}


case class CreateAdaptiveSchemaResponse (
            /* name of resulting adaptive schema */
                  adaptiveSchemaName: String
) extends Response

object CreateAdaptiveSchemaResponse {
  implicit val format: Format[CreateAdaptiveSchemaResponse] = Json.format
}


case class ExplainResponse (
                  reasons: Seq[ReasonSet]
) extends Response

object ExplainResponse {
  implicit val format: Format[ExplainResponse] = Json.format
}


case class ExplainReasonsResponse (
                  reasons: Seq[Reason]
) extends Response

object ExplainReasonsResponse {
  implicit val format: Format[ExplainReasonsResponse] = Json.format
}


case class DataContainer (
                  schema: Seq[Schema],
                  data: Seq[Seq[PrimitiveValue]],
                  prov: Seq[String],
                  colTaint: Seq[Seq[Boolean]],
                  rowTaint: Seq[Boolean],
                  reasons: Seq[Seq[Reason]]
) extends Response

object DataContainer {
  implicit val format: Format[DataContainer] = Json.format
}


case class LensList (
    lensTypes: Seq[String]
) extends Response

object LensList {
  implicit val format: Format[LensList] = Json.format
}


case class AdaptiveSchemaList (
    adaptiveSchemaTypes: Seq[String]
) extends Response

object AdaptiveSchemaList {
  implicit val format: Format[AdaptiveSchemaList] = Json.format
}


case class SchemaList (
    schema: Seq[Schema]
) extends Response

object SchemaList {
  implicit val format: Format[SchemaList] = Json.format
}



