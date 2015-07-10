package controllers

import java.io.File

import mimir.{WebAPI, WebResult, WebQueryResult, WebStringResult}
import play.api.mvc._
import play.api.libs.json._

class Application extends Controller {

  implicit val WebStringResultWrites = new Writes[WebStringResult] {
    def writes(webStringResult: WebStringResult) = Json.obj(
      "result" -> webStringResult.result
    )
  }

  implicit val WebQueryResultWrites = new Writes[WebQueryResult] {
    def writes(webQueryResult: WebQueryResult) = Json.obj(
      "headers" -> webQueryResult.webIterator.header,
      "data" -> webQueryResult.webIterator.data.map(x => x._1),
      "rowValidity" -> webQueryResult.webIterator.data.map(x => x._2),
      "missingRows" -> webQueryResult.webIterator.missingRows
    )
  }

  def index = Action {
    val webAPI = new WebAPI()
    webAPI.configure(new Array[String](0))

    val result: WebResult = new WebStringResult("Query results show up here...")

    generateResponse(webAPI, "", result)
  }

  def query = Action { request =>
    val form = request.body.asFormUrlEncoded
    val query = form.get("query")(0)
    val db = form.get("db")(0)

    val webAPI = new WebAPI()
    var result: WebResult = null
    webAPI.configure(Array("--db", db))

    if(!query.equals("")) {
      result = webAPI.handleStatement(query)
    }
    else {
      result = new WebStringResult("Working database changed to "+db)
    }

    generateResponse(webAPI, query, result)
  }

  def queryGet(query: String, db: String) = Action {

    val webAPI = new WebAPI()
    var result: WebResult = null
    webAPI.configure(Array("--db", db))

    if(!query.equals("")) {
      result = webAPI.handleStatement(query)
    }
    else {
      result = new WebStringResult("Working database changed to "+db)
    }

    generateResponse(webAPI, query, result)
  }

  def queryJson(query: String, db: String) = Action {

    val webAPI = new WebAPI()
    var result: WebResult = null
    webAPI.configure(Array("--db", db))

    result = webAPI.handleStatement(query)

    result match {
      case x: WebStringResult => Ok(Json.toJson(x.asInstanceOf[WebStringResult]))
      case x: WebQueryResult  => Ok(Json.toJson(x.asInstanceOf[WebQueryResult]))
    }
  }

  def createDB = Action { request =>
    val form = request.body.asFormUrlEncoded
    val db = form.get("new_db")(0)

    val webAPI = new WebAPI()
    webAPI.configure(Array("--db", db, "--init"))
    val result: WebResult = new WebStringResult("Database "+db+" successfully created.")

    generateResponse(webAPI, "", result)
  }

  def loadTable = Action(parse.multipartFormData) { request =>
    val webAPI = new WebAPI()
    val db = request.body.dataParts("db")(0)

    request.body.file("file").map { csvFile =>
      val name = csvFile.filename
      val dir = play.Play.application().path().getAbsolutePath()

      val newFile = new File(dir, name)
      csvFile.ref.moveTo(newFile)
      webAPI.configure(Array("--db", db, "--loadTable", name.replace(".csv", "")))
      newFile.delete()
    }

    val result: WebResult = new WebStringResult("CSV file loaded.")

    generateResponse(webAPI, "", result)
  }

  private def generateResponse(webAPI: WebAPI, query: String, result: WebResult): Result = {
    val response: Result = Ok(views.html.index(webAPI, query, result))
    webAPI.close()
    return response
  }
}
