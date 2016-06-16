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


  var webAPI = new WebAPI()


  /*
   * Actions
   */
  def index = Action {
    try {
      webAPI.openBackendConnection()
      val result: WebResult = new WebStringResult("Query results show up here...")
      Ok(views.html.index(webAPI, "", result, ""))
    }
    finally {
      webAPI.closeBackendConnection()
    }
  }


  /**
   * Database selection handlers
   */
  def changeDB = Action { request =>

    val form = request.body.asFormUrlEncoded
    val db = form.get("db").head

    if(!webAPI.getCurrentDB.equalsIgnoreCase(db)) {
      webAPI = new WebAPI(dbName = db)
    }

    try {
      webAPI.openBackendConnection()
      Ok(views.html.index(webAPI, "",
        new WebStringResult("Working database changed to "+db), ""))
    }
    finally {
      webAPI.closeBackendConnection()
    }

  }

  def createDB = Action { request =>

    val form = request.body.asFormUrlEncoded
    val db = form.get("db").head

    webAPI = new WebAPI(dbName = db)

    try {
      webAPI.openBackendConnection()
      webAPI.initializeDBForMimir()
      val result: WebResult = new WebStringResult("Database "+db+" successfully created.")

      Ok(views.html.index(webAPI, "", result, ""))
    }
    finally {
      webAPI.closeBackendConnection()
    }
  }


  /**
   * Query handlers
   */
  def query = Action { request =>
    try {
      webAPI.openBackendConnection()
      val form = request.body.asFormUrlEncoded
      val query = form.get("query")(0)
      val (result, lastQuery) = webAPI.handleStatement(query)
      Ok(views.html.index(webAPI, query, result, lastQuery))
    }
    finally {
      webAPI.closeBackendConnection()
    }
  }

  def queryGet(query: String, db: String) = Action {
    if(!db.equalsIgnoreCase(webAPI.getCurrentDB)) {
      webAPI = new WebAPI(dbName = db)
    }

    try {
      webAPI.openBackendConnection()
      val (result, lastQuery) = webAPI.handleStatement(query)
      Ok(views.html.index(webAPI, query, result, lastQuery))
    }
    finally {
      webAPI.closeBackendConnection()
    }
  }

  def nameForQuery(query: String, db: String) = Action {
    try {
      webAPI.openBackendConnection()
      
      val result = webAPI.nameForQuery(query)

      result match {
        case x: WebStringResult => Ok(Json.toJson(x.asInstanceOf[WebStringResult]))
        case x: WebQueryResult  => Ok(Json.toJson(x.asInstanceOf[WebQueryResult]))
        case x: WebErrorResult  => Ok(Json.toJson(x.asInstanceOf[WebErrorResult]))
      }
    }
    finally {
      webAPI.closeBackendConnection()
    }
  }

  def queryJson(query: String, db: String) = Action {
    if(!db.equalsIgnoreCase(webAPI.getCurrentDB)) {
      webAPI = new WebAPI(dbName = db)
    }

//    webAPI.synchronized(
      try {
        webAPI.openBackendConnection()
        val (result, _) = webAPI.handleStatement(query)

        result match {
          case x: WebStringResult => Ok(Json.toJson(x.asInstanceOf[WebStringResult]))
          case x: WebQueryResult  => Ok(Json.toJson(x.asInstanceOf[WebQueryResult]))
          case x: WebErrorResult  => Ok(Json.toJson(x.asInstanceOf[WebErrorResult]))
        }
      }
      finally {
        webAPI.closeBackendConnection()
      }
//    )
  }


  /**
   * Load CSV data handler
   */
  def loadTable = Action(parse.multipartFormData) { request =>
//    webAPI.synchronized(
      try {
        webAPI.openBackendConnection()

        request.body.file("file").map { csvFile =>
          val name = csvFile.filename
          val dir = play.Play.application().path().getAbsolutePath

          val newFile = new File(dir, name)
          csvFile.ref.moveTo(newFile, true)
          webAPI.handleLoadTable(name)
          newFile.delete()
        }

        val result: WebResult = new WebStringResult("CSV file loaded.")
        Ok(views.html.index(webAPI, "", result, ""))
      }
      finally {
        webAPI.closeBackendConnection()
      }
//    )
  }


  /**
   * Return a list of all tables
   */
  def allTables(db: String) = Action {
    if(!db.equalsIgnoreCase(webAPI.getCurrentDB)) {
      webAPI = new WebAPI(dbName = db)
    }

    try {
      webAPI.openBackendConnection()
      val result = webAPI.getAllDBs
      Ok(Json.toJson(result))
    }
    finally {
      webAPI.closeBackendConnection()
    }
  }

  def getExplainObject(query: String, row: String, ind: String, db: String) = Action {
    if(!db.equalsIgnoreCase(webAPI.getCurrentDB)) {
      webAPI = new WebAPI(dbName = db)
    }

    try {
      webAPI.openBackendConnection()
      val result = webAPI.getExplainObject(query, row, Integer.parseInt(ind))
      Ok(result)
    }
    finally {
      webAPI.closeBackendConnection()
    }

  }

  /**
   * Return a list of all VGTerms present in a particular
   * cell of a query's result
   */
  def getVGTerms(query: String, row: String, ind: String, db: String) = Action {
    if(!db.equalsIgnoreCase(webAPI.getCurrentDB)) {
      webAPI = new WebAPI(dbName = db)
    }

    val i = Integer.parseInt(ind)

//    webAPI.synchronized(
      try {
        webAPI.openBackendConnection()
        val result = webAPI.getVGTerms(query, row, i)
        Ok(Json.toJson(result))
      }
      finally {
        webAPI.closeBackendConnection()
      }
//    )
  }
}
