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

  var nonJsonCount = 0
  val rowLimit = 100000 // 0 is off
  var rowCount = 0
  val addToDB = false

  var rows : ListBuffer[PrimitiveValue] = null
  val NTC = new NaiveTypeCount()


  def loadJson(db: Database, tableName: String, sourceFile: File, columnName: String = "JSONCOLUMN"): Unit = {

    if(addToDB)
      createJsonTable(db,tableName,columnName)

    val updateStatement: String = s"INSERT INTO $tableName($columnName) VALUES (?);"
    rows = ListBuffer[PrimitiveValue]()
    val input = new BufferedReader(new FileReader(sourceFile))
    var line:String = input.readLine()
/*
    while(line != null && ((rowCount < rowLimit) || rowLimit < 1)){ // first try the case where every line is a json object
      if(!line.equals("")) {
        val origLine = line
        try {
          while (line.charAt(0) != '{' || line.charAt(line.size - 1) != '}') { // give it the best shot at being in json format
            if (line.charAt(0) != '{')
              line = line.substring(1, line.size)
            if (line.charAt(line.size - 1) != '}')
              line = line.substring(0, line.size - 1)
          }
          rows += StringPrimitive(line)
          NTC.add(line)
        } catch {
          case e: Exception => // it's not json so just don't add it
            //println(origLine)
            nonJsonCount += 1
        }
        rowCount += 1
        if ((rowCount % 10000) == 0 && !rows.isEmpty) {
//          db.backend.fastUpdateBatch(updateStatement, rows.map((x) => Seq(x)))
          //println(rowCount)
          rows = new ListBuffer[PrimitiveValue]()
        }
      }
        line = input.readLine()
    }
    if(!rows.isEmpty){
//      db.backend.fastUpdateBatch(updateStatement, rows.map((x) => Seq(x)))
      rows = new ListBuffer[PrimitiveValue]()
    }
    input.close()
    */

    //if((nonJsonCount/rowCount) >= .9 || rowCount == 1){
    if(true){
      // this is a last ditch attempt to load the json object, this will not use return carriage so it should be slow
      if(addToDB)
        createJsonTable(db,tableName,columnName)
//      println(s"time to kick the Json Loader into... MAXIMUM OVERDRIVE!!! for table $tableName")

      var indent = 0 // this tracks how far indented in a object you are, if it is at 0 then it is done
      val input2 = new BufferedReader(new FileReader(sourceFile))
      var line: String = input2.readLine()
      var jsonObj: String = "" // holds the json object
      rows = ListBuffer[PrimitiveValue]()
      rowCount = 0
      try {
        line.charAt(0) match {
          case '[' =>
            // probably an array but no return carriage
            while(line != null) {
              val charIter = line.iterator
              while(charIter.hasNext){
                val c = charIter.next()
                c match {
                  case '{' =>
                    indent += 1
                    if (indent > 0)
                      jsonObj += c
                  case '}' =>
                    if (indent > 0)
                      jsonObj += c
                    indent -= 1
                  case _ =>
                    if (indent > 0)
                      jsonObj += c
                    else {
                      add(jsonObj)
                      jsonObj = ""
                      if (rowCount%10000 == 0 && addToDB) {
                        db.backend.fastUpdateBatch(updateStatement, rows.map((x) => Seq(x)))
                        rows = new ListBuffer[PrimitiveValue]()
                      }
                    }

                }
              } // end iterator
              line = input2.readLine()
            }

          case '{' => { // json without new line formatting
            indent += 1 // indent in
            while(line != null && ((rowCount < rowLimit) || rowLimit < 1)) {
              val charIter = line.iterator
              while(charIter.hasNext){
                val c = charIter.next()
                c match {
                  case '{' =>
                    indent += 1
                    jsonObj += c
                  case '}' =>
                    jsonObj += c
                    indent -= 1
                    if (indent == 0) {
                      add(jsonObj)
                      jsonObj = ""
                      if (rowCount%10000 == 0 && addToDB) {
                        db.backend.fastUpdateBatch(updateStatement, rows.map((x) => Seq(x)))
                        rows = new ListBuffer[PrimitiveValue]()
                      }
                    }
                  case _ =>
                    if (indent > 0)
                      jsonObj += c
                }
              } // end iterator
              line = input2.readLine()
            }
          }

          case _ =>
            while(line != null && ((rowCount < rowLimit) || rowLimit < 1)) {
            val charIter = line.iterator
            while(charIter.hasNext){
              val c = charIter.next()
              c match {
                case '{' =>
                  indent += 1
                  if (indent > 0)
                    jsonObj += c
                case '}' =>
                  if (indent > 0)
                    jsonObj += c
                  indent -= 1
                case _ =>
                  if (indent > 0)
                    jsonObj += c
                  else {
                    add(jsonObj)
                    jsonObj = ""
                    if (rowCount%10000 == 0 && addToDB) {
                      db.backend.fastUpdateBatch(updateStatement, rows.map((x) => Seq(x)))
                      rows = new ListBuffer[PrimitiveValue]()
                    }
                  }

              }
            } // end iterator
            line = input2.readLine()
          }
        }
      } catch {
        case e: EOFException => e.printStackTrace()
        // do nothing the file just ended
      }
      if(!rows.isEmpty && addToDB)
        db.backend.fastUpdateBatch(updateStatement, rows.map((x) => Seq(x)))
      input2.close()
    } // end json not working attempt
    println(s"$rowCount rows were added, $nonJsonCount rows were rejected")
    NTC.report()
  }

  def add(s: String): Boolean = {
    val added: Boolean = NTC.add(s)
    if(addToDB && added)
      rows += StringPrimitive(s)
    if(added)
      rowCount += 1
    else
      nonJsonCount += 1
    added
  }

  def createJsonTable(db: Database, tableName: String, columnName: String): Unit = {
    val createTableStatement: String = s"CREATE TABLE $tableName($columnName)"
    db.backend.update(s"DROP TABLE IF EXISTS $tableName;")
    db.backend.update(createTableStatement)
  }
}
