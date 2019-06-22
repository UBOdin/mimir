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
            /* optionally provide a name */
                  humanReadableName: Option[String],
            /* options for spark datasource api */
                  backendOption: Seq[Tuple]
) extends Request {
  def handle(os:OutputStream) = {
    os.write(Json.stringify(Json.toJson(LoadResponse(MimirVizier.loadDataSource(file, format, inferTypes, detectHeaders, humanReadableName, backendOption.map(tup => tup.name -> tup.value))))).getBytes )
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
                  input: Map[String,String],
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

case class CreateViewSRequest (
            /* input for view */
                  input: String,
            /* query for view */
                  query: String
)  extends Request {
  def handle(os:OutputStream) = {
    os.write(Json.stringify(Json.toJson(CreateViewResponse(MimirVizier.createView(input, query)))).getBytes )
  }
}

object CreateViewSRequest {
  implicit val format: Format[CreateViewSRequest] = Json.format
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


case class ExplainSubsetWithoutSchemaRequest (
            /* query to explain */
                  query: String,
                  rows: Seq[String],
                  cols: Seq[String]
) extends Request {
  def handle(os:OutputStream) = {
    os.write(Json.stringify(Json.toJson(ExplainResponse(MimirVizier.explainSubsetWithoutSchema(query, rows, cols).map(rsn => 
      ReasonSet(rsn.model.name.toString, rsn.idx, rsn.argLookup match {
        case Some((query, args, hints)) => "[" + args.mkString(", ") + "][" + hints.mkString(", ") + "] <- \n" + query.toString("   ")
        case None => ""
      })
    )))).getBytes )
  }
}

object ExplainSubsetWithoutSchemaRequest {
  implicit val format: Format[ExplainSubsetWithoutSchemaRequest] = Json.format
}


case class ExplainSchemaRequest (
            /* query to explain */
                  query: String,
                  cols: Seq[String]
) extends Request {
  def handle(os:OutputStream) = {
    os.write(Json.stringify(Json.toJson(ExplainResponse(MimirVizier.explainSchema(query, cols).map(rsn => 
      ReasonSet(rsn.model.name.toString, rsn.idx, rsn.argLookup match {
        case Some((query, args, hints)) => "[" + args.mkString(", ") + "][" + hints.mkString(", ") + "] <- \n" + query.toString("   ")
        case None => ""
      })
    )))).getBytes )
  }
}

object ExplainSchemaRequest {
  implicit val format: Format[ExplainSchemaRequest] = Json.format
}


case class ExplainCellSchemaRequest (
            /* query to explain */
                  query: String,
            /* rowid of cell */
                  row: String,
            /* column of cell */
                  col: Int
) extends Request {
  def handle(os:OutputStream) = {
    os.write(Json.stringify(Json.toJson(ExplainReasonsResponse(MimirVizier.explainCell(query, col, row).map(rsn => 
      Reason(rsn.reason, rsn.model.name.toString, rsn.idx, rsn.args.map(_.toString()), mimir.api.Repair(rsn.repair.toJSON), rsn.repair.exampleString)
    )))).getBytes )
  }
}

object ExplainCellSchemaRequest {
  implicit val format: Format[ExplainCellSchemaRequest] = Json.format
}


case class ExplainSubsetRequest (
            /* query to explain */
                  query: String,
                  rows: Seq[String],
                  cols: Seq[String]
) extends Request {
  def handle(os:OutputStream) = {
    os.write(Json.stringify(Json.toJson(ExplainResponse(MimirVizier.explainSubset(query, rows, cols.map(mimir.algebra.ID(_))).map(rsn => 
      ReasonSet(rsn.model.name.toString, rsn.idx, rsn.argLookup match {
        case Some((query, args, hints)) => "[" + args.mkString(", ") + "][" + hints.mkString(", ") + "] <- \n" + query.toString("   ")
        case None => ""
      })
    )))).getBytes )
  }
}

object ExplainSubsetRequest {
  implicit val format: Format[ExplainSubsetRequest] = Json.format
}


case class ExplainEverythingAllRequest (
            /* query to explain */
                  query: String
) extends Request {
  def handle(os:OutputStream) = {
    os.write(Json.stringify(Json.toJson(
      ExplainReasonsResponse(
        MimirVizier.explainEverythingAll(query).map { 
          rsn => 
            Reason(
              rsn.reason, 
              rsn.model.name.toString, 
              rsn.idx, 
              rsn.args.map(_.toString()), 
              mimir.api.Repair(rsn.repair.toJSON), 
              rsn.repair.exampleString
            )
        }
      )
    )).getBytes )
  }
}

object ExplainEverythingAllRequest {
  implicit val format: Format[ExplainEverythingAllRequest] = Json.format
}


case class ExplainEverythingRequest (
            /* query to explain */
                  query: String
) extends Request {
  def handle(os:OutputStream) = {
    os.write(Json.stringify(Json.toJson(
      ExplainResponse(
        MimirVizier.explainEverything(query).map { 
          rsn => 
            ReasonSet(
              rsn.model.name.toString, 
              rsn.idx, 
              rsn.argLookup match {
                case Some((query, args, hints)) => "[" + args.mkString(", ") + "][" + hints.mkString(", ") + "] <- \n" + query.toString("   ")
                case None => ""
              }
            )
        }
      )
    )).getBytes )
  }
}

object ExplainEverythingRequest {
  implicit val format: Format[ExplainEverythingRequest] = Json.format
}


case class FeedbackForReasonRequest (
                  reason: Reason,
            /* idx */
                  idx: Int,
            /* acknowledge guess */
                  ack: Boolean,
            /* repair string */
                  repairStr: String
) extends Request {
  def handle(os:OutputStream) = {
    val (model, argsHints) = (reason.source, reason.args)
    MimirVizier.feedback(model, idx, argsHints, ack, repairStr)
  }
}

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
                  includeReasons: Boolean
) extends Request {
  def handle(os:OutputStream) = {
    os.write(Json.stringify(Json.toJson(MimirVizier.vistrailsQueryMimir(input, query, includeUncertainty, includeReasons) )).getBytes )
  }
}

object QueryMimirRequest {
  implicit val format: Format[QueryMimirRequest] = Json.format
}


case class SchemaForQueryRequest (
            /* query string to get schema for - sql */
                  query: String
) extends Request {
  def handle(os:OutputStream) = {
    os.write(Json.stringify(Json.toJson(SchemaList(MimirVizier.getSchema(query)))).getBytes )
  }
}

object SchemaForQueryRequest {
implicit val format: Format[SchemaForQueryRequest] = Json.format
}

