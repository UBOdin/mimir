package controllers

import java.io.File

import mimir._
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

  implicit val WebErrorResultWrites = new Writes[WebErrorResult] {
    def writes(webErrorResult: WebErrorResult) = Json.obj(
      "error" -> webErrorResult.result
    )
  }

  def index = Action {
    val webAPI = new WebAPI()
    webAPI.configure(new Array[String](0))

    val result: WebResult = new WebStringResult("Query results show up here...")

    val response: Result = Ok(views.html.index(webAPI, "", result, ""))
    webAPI.close()
    response
  }

  def query = Action { request =>
    val form = request.body.asFormUrlEncoded
    val query = form.get("query")(0)
    val db = form.get("db")(0)

    val webAPI = new WebAPI()
    var result: WebResult = null
    var lastQuery: String = ""
    webAPI.configure(Array("--db", db))

    if(!query.equals("")) {
      val ret = webAPI.handleStatement(query)
      result = ret._1
      lastQuery = ret._2
    }
    else {
      result = new WebStringResult("Working database changed to "+db)
    }

    val response: Result = Ok(views.html.index(webAPI, query, result, lastQuery))
    webAPI.close()
    response
  }

  def queryGet(query: String, db: String) = Action {

    val webAPI = new WebAPI()
    var result: WebResult = null
    var lastQuery: String = ""

    webAPI.configure(Array("--db", db))

    if(!query.equals("")) {
      val ret = webAPI.handleStatement(query)
      result = ret._1
      lastQuery = ret._2
    }
    else {
      result = new WebStringResult("Working database changed to "+db)
    }

    val response: Result = Ok(views.html.index(webAPI, query, result, lastQuery))
    webAPI.close()
    response
  }

  def queryJson(query: String, db: String) = Action {

    val webAPI = new WebAPI()
    var result: WebResult = null
    webAPI.configure(Array("--db", db))

    val ret = webAPI.handleStatement(query)
    result = ret._1

    val response = result match {
      case x: WebStringResult => Ok(Json.toJson(x.asInstanceOf[WebStringResult]))
      case x: WebQueryResult  => Ok(Json.toJson(x.asInstanceOf[WebQueryResult]))
      case x: WebErrorResult  => Ok(Json.toJson(x.asInstanceOf[WebErrorResult]))
    }

    webAPI.close()
    response
  }

  def createDB = Action { request =>
    val form = request.body.asFormUrlEncoded
    val db = form.get("new_db")(0)

    val webAPI = new WebAPI()
    webAPI.configure(Array("--db", db, "--init"))
    val result: WebResult = new WebStringResult("Database "+db+" successfully created.")

    val response: Result = Ok(views.html.index(webAPI, "", result, ""))
    webAPI.close()
    response
  }

  def loadTable = Action(parse.multipartFormData) { request =>
    val webAPI = new WebAPI()
    val db = request.body.dataParts("db")(0)

    request.body.file("file").map { csvFile =>
      val name = csvFile.filename
      val dir = play.Play.application().path().getAbsolutePath()

      val newFile = new File(dir, name)
      csvFile.ref.moveTo(newFile, true)
      webAPI.configure(Array("--db", db, "--loadTable", name.replace(".csv", "")))
      newFile.delete()
    }

    val result: WebResult = new WebStringResult("CSV file loaded.")

    val response: Result = Ok(views.html.index(webAPI, "", result, ""))
    webAPI.close()
    response
  }

  def allTables(db: String) = Action {
    val webAPI = new WebAPI()
    webAPI.configure(Array("--db", db))

    val result = webAPI.getAllDBs()

    val response = Ok(Json.toJson(result))
    webAPI.close()
    response
  }

  def getVGTerms(query: String, row: String, ind: String, db: String) = Action {
    val webAPI = new WebAPI()
    webAPI.configure(Array("--db", db))

    val i = Integer.parseInt(ind)
    val result = webAPI.getVGTerms(query, row, i)

    val response = Ok(Json.toJson(result))
    webAPI.close()
    response
  }
}
