package mimir.util

import mimir.Database
import play.api.libs.json._
import org.apache.commons.text.StringEscapeUtils._
import java.io.{BufferedReader, EOFException, File, FileReader}

import mimir.algebra.{PrimitiveValue, StringPrimitive}

import scala.collection.mutable.ListBuffer

/**
  * Created by Will on 7/26/2017.
  */
object LoadJSON {


  def loadJson(db: Database, tableName: String, sourceFile: File, columnName: String = "JSONCOLUMN"): Unit = {
    // create the table
    var nonJsonCount = 0
    var rowCount = 0

    createJsonTable(db,tableName,columnName)

    val input = new BufferedReader(new FileReader(sourceFile))
    var line:String = input.readLine()
    while(line != null && line != ""){
      try {
        while (line.charAt(0) != '{' || line.charAt(line.size - 1) != '}') { // give it the best shot at being in json format
          if (line.charAt(0) != '{')
            line = line.substring(1, line.size)
          if (line.charAt(line.size - 1) != '}')
            line = line.substring(0, line.size - 1)
        }
        db.backend.update(s"INSERT INTO $tableName($columnName) VALUES ('$line');")
        line = input.readLine()
      } catch {
        case e: Exception => // it's not json so just don't add it
          line = input.readLine() // try the next line
          nonJsonCount += 1
      }
      rowCount += 1
    }
    input.close()

    if((nonJsonCount/rowCount) >= .9){
      // this is a last ditch attempt to load the json object, this will not use return carriage so it should be slow
      db.backend.update(s"DROP TABLE $tableName;")
      createJsonTable(db,tableName,columnName)

      var indent = 0 // this tracks how far indented in a object you are, if it is at 1 then it is done
      val input = new BufferedReader(new FileReader(sourceFile))
      var line: String = input.readLine()
      var jsonObj: String = "" // holds the json object
      val updateStatement: String = s"INSERT INTO $tableName($columnName) VALUES (?);"
      var rows : ListBuffer[PrimitiveValue] = new ListBuffer[PrimitiveValue]()
      var rowCount = 0
      try {
        line.charAt(0) match {
          case '[' =>
            // probably an array but no return carriage
            while(!line.equals("")) {
              val charIter = line.iterator
              while(charIter.hasNext){
                val c = charIter.next()
                c match {
                  case '{' =>
                    indent += 1
                    if (indent > 0)
                      jsonObj += c
                  case '}' =>
                    indent -= 1
                    if (indent > 0)
                      jsonObj += c
                  case ',' => // if indent is at 1 then stop object
                    if (indent == 1) {
                      rows += StringPrimitive(jsonObj)
                      jsonObj = ""
                      rowCount += 1
                      if (rowCount >= 69435) {
                        db.backend.fastUpdateBatch(updateStatement, rows.map((x) => Seq(x)))
                        rows = new ListBuffer[PrimitiveValue]()
                        rowCount = 0
                      }
                    }
                    else
                      jsonObj += c
                  case _ =>
                    if (indent > 0)
                      jsonObj += c
                }
              } // end iterator
              println("next line")
              line = ""
            }

          case _ => throw new EOFException("End of file")
        }
      } catch {
        case e: EOFException => // do nothing the file just ended
      }
      input.close()
      println("done")
    } // end json not working attempt

  }

  def createJsonTable(db: Database, tableName: String, columnName: String): Unit = {
    // create the table
    val createTableStatement: String = s"CREATE TABLE $tableName($columnName)"
    db.backend.update(createTableStatement)
  }
}
