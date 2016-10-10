package mimir.util

import java.io._
import java.nio.charset.StandardCharsets
import java.sql.SQLException
import java.util

import mimir.Database
import mimir.algebra.Type
import org.apache.commons.csv.{CSVRecord, CSVParser, CSVFormat}
import org.apache.commons.io.IOUtils
import org.apache.commons.io.input.ReaderInputStream
import scala.collection.JavaConverters._
import mimir.algebra._

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.Breaks._

object LoadCSV {

  def handleLoadTable(db: Database, targetTable: String, sourceFile: File){
    var largeData = false
    var createTableStatement = "CREATE TABLE " + targetTable + "("
    var numberOfHeaderColumns = 0
    var maxNumberOfColumns = 0

    var input = new BufferedReader(new FileReader(sourceFile))

    val cleanedInputStream: BufferedReader = makeColumnNamesUnique(input)
    val parser = new CSVParser(cleanedInputStream , CSVFormat.DEFAULT.withAllowMissingColumnNames().withHeader())


    // reset back to the top
    input = new BufferedReader(new FileReader(sourceFile))

    db.getTableSchema(targetTable) match {

      case Some(sch) =>
        if(true) {
//          input.readLine()
          populateTable(db, parser, targetTable, sch) // Ignore header since table already exists
        }
        else {
          populateTable(
            db,
            parser, // Reset to top
            targetTable,
            sch
          )
        }

      case None =>
        val firstLine = "" //input.readLine()

        if(largeData) { // if the data is large we may need to sacrafice error checking for speed
          if (headerDetected(firstLine)) {
            db.backend.update("CREATE TABLE " + targetTable + "(" +
              firstLine.split(",").map((x) => "\'" + x.trim.replace(" ", "") + "\'").mkString(" varchar, ") +
              " varchar)")

            handleLoadTable(db, targetTable, sourceFile)
          }
          else {
            throw new SQLException("No header supplied for creating new table")
          }
        }
        else { // first time around get the max number of columns and make sure it matches up to the number of columns in the schema
          if(headerDetected(firstLine)){

//            numberOfHeaderColumns = firstLine.split(",").length

            var maxNumberOfColumns = 0
            val parser: CSVParser = new CSVParser(makeColumnNamesUnique(input), CSVFormat.DEFAULT.withAllowMissingColumnNames().withHeader())

            for (row: CSVRecord <- parser.asScala) {
              maxNumberOfColumns = Math.max(0, row.size())
            }


//            val delta = maxNumberOfColumns - numberOfHeaderColumns
            val delta = 0
            var args:String = ""
            val colList: util.ArrayList[String] = new util.ArrayList[String]()
            var counter = 0

            val columnHeaderMap = parser.getHeaderMap.asScala
            var columnHeadersInOrder = new Array[String](columnHeaderMap.size)
            for ((columnHeader, index) <- columnHeaderMap) {
              columnHeadersInOrder(index) = columnHeader + " varchar"
            }

            args += columnHeadersInOrder.mkString(", ")

//            for (col <- columnHeadersInOrder) {
//
//            }
//
//
//            firstLine.split(",").map((x) => {
//              var col = x.trim.replaceAll(" ", "").replaceAll("&","").replaceAll("\\(","").replaceAll("\\)","").replaceAll("\'","").replaceAll("\\.","").replaceAll("/","")
//              if(col.toUpperCase().equals("DATE")){
//                col = "date1"
//              }
//              if(col.toUpperCase().equals("MONTH")){
//                col = "month1"
//              }
//              while(colList.contains(col)){
//                col = col + counter
//                counter += 1
//              }
//              colList.add(col)
//              args += col + " varchar, "
//            }
//            )

            createTableStatement += args

            if(delta > 1) {
              for (i <- 0 until delta-1) {
                createTableStatement += "col" + i + " varchar, "
              }
              createTableStatement += "col" + (delta-1) + " varchar)"
            }
            else if(delta == 1){
              createTableStatement += " col" + " varchar)"
            }
            else{
              createTableStatement = createTableStatement.substring(0,createTableStatement.size-2)
              createTableStatement += ")"
            }
            db.backend.update(createTableStatement)
            handleLoadTable(db, targetTable, sourceFile)

          }
          else{ // no header so make one
            println("Should not be here right now")
            while(true) {
              val line: String = input.readLine()
              if(line == null) {
                break
              }
              if(line.split(",").length > maxNumberOfColumns){ // needs to be smarter but for right now use this
                maxNumberOfColumns = line.split(",").length
              }
            }

            for(x <- 0 until maxNumberOfColumns - 1){
              createTableStatement += "col" + x + " varchar, "
            }
            createTableStatement += "col" + (maxNumberOfColumns - 1) + " varchar)"
            db.backend.update(createTableStatement)
            handleLoadTable(db, targetTable, sourceFile)
          }
        }
    }
  }


  private def makeColumnNamesUnique(src: BufferedReader): BufferedReader = {
    val headerLine: String = src.readLine()

    val originalHeaders: Array[String] = headerLine.split(',')
    var modifiedHeaders = new Array[String](0)

    for (originalHeader <- originalHeaders) {
      val cleanedOriginalHeader = originalHeader.replaceAll(" ", "").replaceAll("&","").replaceAll("\\(","").replaceAll("\\)","").replaceAll("\'","").replaceAll("\\.","").replaceAll("/","").replaceAll("\\%","").replaceAll("\\-","")

      var desiredHeader = cleanedOriginalHeader
      var attemptCount = 0

      while(modifiedHeaders.contains(desiredHeader)) {
        attemptCount = attemptCount + 1
        desiredHeader = cleanedOriginalHeader + "_" + attemptCount
      }

      modifiedHeaders = modifiedHeaders :+ desiredHeader
    }

    val modifiedHeaderLine = modifiedHeaders.mkString(",")


    val headerLineInputStream: InputStream = IOUtils.toInputStream(modifiedHeaderLine + '\n', StandardCharsets.UTF_8)
    val restOfFileInputStream: ReaderInputStream = new ReaderInputStream(src, StandardCharsets.UTF_8)

    // java is gross...
    return new BufferedReader(new InputStreamReader(new SequenceInputStream(headerLineInputStream, restOfFileInputStream)))

  }

  /**
   * A placeholder method for an unimplemented feature
   *
   * During CSV load, a file may have a header, or not
   */
  private def headerDetected(line: String): Boolean = {
    if(line == null) return false

    // TODO Detection logic

    true // For now, assume every CSV file has a header
  }

  private def populateTable(db: Database,
                            parser: CSVParser,
                            targetTable: String,
                            sch: List[(String, Type)]): Unit = {

    var location = 0
    var numberOfColumns = 0
    val keys = sch.map(_._1).map((x) => {numberOfColumns+= 1; "\'"+x+"\'"}).mkString(", ")
    val statements = new ListBuffer[String]()


    for (row: CSVRecord <- parser.asScala) {
        if (row.isConsistent) {


          var columnCount = 0

          val listOfValues: List[String] = row.iterator().asScala.toList
          var data = listOfValues.map((i) => {
            columnCount = columnCount + 1

            i match {
              case "" => null
              case x => sch(columnCount - 1)._2 match {
                case TDate() | TString() => "\'" + x.replaceAll("'", "''") + "\'"
                case _ => x
              }
            }
          })

          data = data.padTo(numberOfColumns, null)
          if (data.length != numberOfColumns) {
            // TODO do something
            var foo = 0;
          }

          val dataString = data.mkString(", ")

//          statements.append("INSERT INTO " + targetTable + "(" + keys + ") VALUES (" + dataString + ")")

          db.backend.update("INSERT INTO " + targetTable + "(" + keys + ") VALUES (" + dataString + ")")
        }
    }

/*
    while(true){

      val line = src.readLine()
      if(line == null) { if(statements.nonEmpty) db.backend.update(statements.toList); return }

      var data = dataLine.indices.map( (i) =>{
        location += 1
        dataLine(i) match {
          case "" => null
          case x => sch(i)._2 match {
            case TDate() | TString() => "\'"+x+"\'"
            case _ => x
          }
        }
    }).mkString(", ")

      if(location < numberOfColumns){
        for(i <- location to numberOfColumns){
          data = data + " NULL,"
        }
      }
      location = 0

//      statements.append("INSERT INTO "+targetTable+"("+keys+") VALUES ("+data+")")
      db.backend.update("INSERT INTO "+targetTable+"("+keys+") VALUES ("+data+")")
    }
*/
  }
}
