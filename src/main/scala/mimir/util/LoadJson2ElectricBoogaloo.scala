package mimir.util

import mimir.Database
import play.api.libs.json._
import org.apache.commons.text.StringEscapeUtils._
import java.io.{BufferedReader, EOFException, File, FileReader}

import mimir.algebra.{PrimitiveValue, StringPrimitive}

import scala.collection.mutable.ListBuffer


object LoadJson2ElectricBoogaloo {

  var nonJsonCount = 0
  var rowLimit = 0 // 0 is off
  var rowCount = 0
  var addToDB = false
  var tableName = ""
  var columnName = ""
  var db: Database = null
  var sourceFile: File = null
  var updateStatement: String = ""

  var rows : ListBuffer[PrimitiveValue] = null
  val NTC = new NaiveTypeCount()


  /*
   escaped: uses escape characters
   rowed: each new line is a row
  */
  def loadJson(db: Database, tableName: String, sourceFile: File, columnName: String = "JSONCOLUMN", addToDB: Boolean = false, rowLimit: Int = 0, escaped: Boolean = false, naive: Boolean = false, rowed: Boolean = true): Unit = {

    // reset all variables in case called multiple times for different tables
    this.addToDB = addToDB
    this.rowLimit = rowLimit
    this.rowCount = 0
    this.nonJsonCount = 0
    this.columnName = columnName
    this.tableName = tableName
    this.rows = ListBuffer[PrimitiveValue]()
    this.db = db
    this.sourceFile = sourceFile
    this.updateStatement = s"INSERT INTO $tableName($columnName) VALUES (?);"
    if(addToDB)
      createJsonTable()

    if(rowed){
      everyNewLine(escaped, naive)
    } else{

    }

    println(s"$rowCount rows were added, $nonJsonCount rows were rejected")
//    NTC.report()
    NTC.reduce()
//    NTC.xFinal()
  }

  private def add(s: String): Boolean = {
    val added: Boolean = NTC.map(s)//NTC.add(s)
    if(addToDB && added) {
      rows += StringPrimitive(s)
      db.backend.fastUpdateBatch(updateStatement, rows.map((x) => Seq(x)))
      rows = new ListBuffer[PrimitiveValue]()
    }
    if(added) {
//      NTC.xStep(s)
      rowCount += 1
    }
    else
      nonJsonCount += 1
    if(rowCount%100000 == 0 && added) {
      println(s"$rowCount rows added, $nonJsonCount rows rejected")
    }
    added
  }

  private def createJsonTable(): Unit = {
    val createTableStatement: String = s"CREATE TABLE $tableName($columnName)"
    db.backend.update(s"DROP TABLE IF EXISTS $tableName;")
    db.backend.update(createTableStatement)
  }


  // called when every line is a json object
  private def everyNewLine(escaped: Boolean, naive: Boolean) = {
    val input = new BufferedReader(new FileReader(sourceFile))
    var line: String = input.readLine()
    while(line != null && ((rowCount < rowLimit) || rowLimit < 1)) {
      if(escaped){ // need to unescape string first

      } else if(!escaped && naive){ // raw input
        try {
          add(line)
        } catch {
          case e: StringIndexOutOfBoundsException =>
            nonJsonCount += 1
        }
      } else if(!escaped && !naive){ // raw input
        try {
          val jsonLine: String = line.substring(line.indexOf('{'), line.lastIndexOf('}') +1)
          add(jsonLine)
        } catch {
          case e: StringIndexOutOfBoundsException =>
            nonJsonCount += 1
        }
      }

      line = input.readLine()
    }
  }
}
