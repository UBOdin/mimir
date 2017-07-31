package mimir.util

import mimir.Database

import java.io.{File, FileReader, BufferedReader}

/**
  * Created by Will on 7/26/2017.
  */
object LoadJSON {

  def handleSingleJSON(db: Database, targetTable: String, sourceFile: File):Unit = {
    val input = new BufferedReader(new FileReader(sourceFile))

    // create the table
    val columnName:String = input.readLine().replace(",","")
    val createTableStatement: String = s"CREATE TABLE $targetTable($columnName)"
    db.backend.update(createTableStatement)

    // populate the table with JSON
    var line:String = input.readLine()
    while(line != null && line != ""){
      db.backend.update(s"INSERT INTO $targetTable($columnName) VALUES ('$line');")
      line = input.readLine()
    }
  }
}
