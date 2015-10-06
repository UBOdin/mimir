package controllers

import java.io.File

import mimir._
import mimir.web.{WebResult, WebErrorResult, WebQueryResult, WebStringResult}
import play.api.mvc._
import play.api.libs.json._

/*
 * This is the entry-point to the Web Interface.
 * This class is part of the template provided by the play framework.
 *
 * Each Action, part of the Play API is a function that maps
 * a Request to a Result. Every URL that can be handled by the
 * application is defined in the routes.conf file. Each url
 * has an Action associated with it, which defines what the
 * server should do when the client asks for that URL
 *
 * GET requests pass in args that can be directly extracted
 * from the Action method signatures. POST requests need parsers.
 * For example, if the POST request had form fields attached to
 * it, we use a form body parser
 *
 * Read more about Actions at
 * https://www.playframework.com/documentation/2.0/ScalaActions
 */

class Application extends Controller {

  /*
   * The Writes interface allows us to convert
   * Scala objects to a JSON representation
   */
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
      "missingRows" -> webQueryResult.webIterator.missingRows,
      "qyeryFlow" -> webQueryResult.webIterator.queryFlow.toJson().toString()
    )
  }

  implicit val WebErrorResultWrites = new Writes[WebErrorResult] {
    def writes(webErrorResult: WebErrorResult) = Json.obj(
      "error" -> webErrorResult.result
    )
  }

  implicit val ReasonWrites = new Writes[(String, String)] {
    def writes(tup: (String, String)) = Json.obj("reason" -> tup._1, "lensType" -> tup._2)
  }



  /*
   * Actions
   */
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

    val result = webAPI.getAllDBs

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
