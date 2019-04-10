package mimir.api
import play.api.libs.json._
import mimir.MimirVizier
import java.io.OutputStream

sealed abstract class Request {
  def handle(os:OutputStream) : Unit
}

object Request {
  
}

case class ScalaEvalRequest (
            /* scala source code to evaluate*/
                  source: String
) extends Request {
  def handle(os:OutputStream) = {
    os.write(Json.stringify(Json.toJson(MimirVizier.evalScala(source))).getBytes )
  }
}

object ScalaEvalRequest {
  implicit val format: Format[ScalaEvalRequest] = Json.format
}


case class LoadRequest (
            /* file url of datasorce to load */
                  file: String,
            /* format of file for spark */
                  format: String,
            /* infer types in data source */
                  inferTypes: Boolean,
            /* detect headers in datasource */
                  detectHeaders: Boolean,
            /* options for spark datasource api */
                  backendOption: Seq[Tuple]
) extends Request {
  def handle(os:OutputStream) = {
    os.write(Json.stringify(Json.toJson(LoadResponse(MimirVizier.loadDataSource(file, format, inferTypes, detectHeaders, backendOption.map(tup => tup.name -> tup.value))))).getBytes )
  }
}

object LoadRequest {
  implicit val format: Format[LoadRequest] = Json.format
}


case class UnloadRequest (
            /* table or view to unload */
                  input: String,
            /* file url of datasorce to unload */
                  file: String,
            /* format of file for spark */
                  format: String,
            /* options for spark datasource api */
                  backendOption: Seq[Tuple]
) extends Request {
  def handle(os:OutputStream) = {
    MimirVizier.unloadDataSource(input, file, format, backendOption.map(tup => tup.name -> tup.value))
  }
}

object UnloadRequest {
  implicit val format: Format[UnloadRequest] = Json.format
}


case class CreateLensRequest (
            /* input for lens */
                  input: String,
                  params: Seq[String],
            /* type name of lens */
                  `type`: String,
            /* materialize input before creating lens */
                  materialize: Boolean
) extends Request {
  def handle(os:OutputStream) = {
    os.write(Json.stringify(Json.toJson(MimirVizier.createLens(input, params, `type`, false, materialize))).getBytes )
  }
}

object CreateLensRequest {
  implicit val format: Format[CreateLensRequest] = Json.format
}


case class CreateViewRequest (
            /* input for view */
                  input: String,
            /* query for view */
                  query: String
)  extends Request {
  def handle(os:OutputStream) = {
    os.write(Json.stringify(Json.toJson(CreateViewResponse(MimirVizier.createView(input, query)))).getBytes )
  }
}

object CreateViewRequest {
  implicit val format: Format[CreateViewRequest] = Json.format
}


case class CreateAdaptiveSchemaRequest (
            /* input for adaptive schema */
                  input: String,
                  params: Seq[String],
            /* type name of adaptive schema */
                  `type`: String
) extends Request {
  def handle(os:OutputStream) = {
    os.write(Json.stringify(Json.toJson(CreateAdaptiveSchemaResponse(MimirVizier.createAdaptiveSchema(input, params, `type`)))).getBytes )
  }
}

object CreateAdaptiveSchemaRequest {
  implicit val format: Format[CreateAdaptiveSchemaRequest] = Json.format
}


case class ExplainCellSchemaRequest (
            /* query to explain */
                  query: String,
            /* rowid of cell */
                  row: String,
            /* column of cell */
                  col: String
)

object ExplainCellSchemaRequest {
  implicit val format: Format[ExplainCellSchemaRequest] = Json.format
}

case class ExplainEverythingAllRequest (
            /* query to explain */
                  query: String
)

object ExplainEverythingAllRequest {
  implicit val format: Format[ExplainEverythingAllRequest] = Json.format
}

case class ExplainEverythingRequest (
            /* query to explain */
                  query: Option[String]
)

object ExplainEverythingRequest {
  implicit val format: Format[ExplainEverythingRequest] = Json.format
}

case class ExplainSchemaRequest (
            /* query to explain */
                  query: String,
                  cols: Seq[String]
)

object ExplainSchemaRequest {
  implicit val format: Format[ExplainSchemaRequest] = Json.format
}

case class ExplainSubsetRequest (
            /* query to explain */
                  query: String,
                  rows: Seq[String],
                  cols: Seq[String]
)

object ExplainSubsetRequest {
  implicit val format: Format[ExplainSubsetRequest] = Json.format
}

case class ExplainSubsetWithoutSchemaRequest (
            /* query to explain */
                  query: String,
                  rows: Seq[String],
                  cols: Seq[String]
)

object ExplainSubsetWithoutSchemaRequest {
  implicit val format: Format[ExplainSubsetWithoutSchemaRequest] = Json.format
}

case class FeedbackForReasonRequest (
                  reasons: Seq[Reason],
            /* idx */
                  idx: Long,
            /* acknowledge guess */
                  ack: Boolean,
            /* repair string */
                  repairStr: String
)

object FeedbackForReasonRequest {
  implicit val format: Format[FeedbackForReasonRequest] = Json.format
}


case class QueryMimirRequest (
            /* input for query */
                  input: String,
            /* query string - sql */
                  query: String,
            /* include taint in response */
                  includeUncertainty: Boolean,
            /* include reasons in response */
                  includeReasons: Boolean,
            /* options for spark datasource api */
                  backendOption: Seq[Tuple]
)

object QueryMimirRequest {
  implicit val format: Format[QueryMimirRequest] = Json.format
}

case class RepairFromReasonRequest (
                  reasons: Seq[Reason],
            /* idx */
                  idx: Long
)

object RepairFromReasonRequest {
  implicit val format: Format[RepairFromReasonRequest] = Json.format
}

