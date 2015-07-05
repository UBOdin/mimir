package controllers

import java.io.File
import java.nio.file.Path

import play.api._
import play.api.mvc._
import mimir.{WebAPI, WebQueryResult}
import play.api.Play.current

class Application extends Controller {


  def index = Action {
    val webAPI = new WebAPI()
    webAPI.configure(new Array[String](0))

    val result = new WebQueryResult(false, "Query results show up here...", null)
    val ret = Ok(views.html.index("", webAPI.dbName, result, webAPI))
    webAPI.close()
    ret

  }

  def input(query: String, db: String) = Action {
    val webAPI = new WebAPI()
    var result: WebQueryResult = null
    webAPI.configure(Array("--db", db))

    if(!query.equals("")) {
      result = webAPI.handleStatement(query)
    }
    else {
      result = new WebQueryResult(false, "Working database changed to "+db, null)
    }

    val ret = Ok(views.html.index(query, db, result, webAPI))
    webAPI.close()
    ret
  }

  def createDB(db: String) = Action {
    val webAPI = new WebAPI()
    webAPI.configure(Array("--db", db, "--init"))
    val result = new WebQueryResult(false, "Database "+db+" successfully created.", null)

    val ret = Ok(views.html.index("", db, result, webAPI))
    webAPI.close()
    ret
  }

  def loadTable() = Action(parse.multipartFormData) { request =>
    val webAPI = new WebAPI()
    request.body.file("file").map { csvFile =>
      val name = csvFile.filename
      val dir = play.Play.application().path().getAbsolutePath()

      val newFile = new File(dir, name)
      csvFile.ref.moveTo(newFile)
      webAPI.configure(Array("--db", webAPI.dbName, "--loadTable", name.replace(".csv", "")))
      newFile.delete()
    }

    val result = new WebQueryResult(false, "CSV file loaded.", null)
    val ret = Ok(views.html.index("", webAPI.dbName, result, webAPI))
    webAPI.close()
    ret
  }
}
