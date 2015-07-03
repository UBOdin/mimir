package controllers

import java.io.{File, StringReader}

import play.api._
import play.api.mvc._
import mimir.{WebAPI, Mimir, WebQueryResult}

class Application extends Controller {

  val webAPI = new WebAPI()
  webAPI.configure(new Array[String](0))

  def index = Action {
    val result = new WebQueryResult(false, "Query results show up here...", null)
    Ok(views.html.index("", webAPI.dbName, result, webAPI))
  }

  def input(query: String, db: String) = Action {
    var result: WebQueryResult = null
    webAPI.configure(Array("--db", db))

    if(!query.equals("")) {
      result = webAPI.handleStatement(query)
    }
    else {
      result = new WebQueryResult(false, "Working database changed to "+db, null)
    }

    Ok(views.html.index(query, db, result, webAPI))
  }

  def createDB(db: String) = Action {
    webAPI.configure(Array("--db", db, "--init"))
    val result = new WebQueryResult(false, "Database "+db+" successfully created.", null)

    Ok(views.html.index("", db, result, webAPI))
  }

  def loadTable() = Action(parse.multipartFormData) { request =>
    request.body.file("file").map { csvFile =>
      val name = csvFile.filename
      val newFile = new File("/home/arindam/Documents/MimirWebApp/" + name)
      csvFile.ref.moveTo(newFile)
      webAPI.configure(Array("--db", webAPI.dbName, "--loadTable", name.replace(".csv", "")))
      newFile.delete()
    }

    val result = new WebQueryResult(false, "CSV file loaded.", null)
    Ok(views.html.index("", webAPI.dbName, result, webAPI))
  }
}
