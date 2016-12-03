package mimir.util

import com.typesafe.scalalogging.slf4j.LazyLogging

import java.io.{File, FileReader, BufferedReader}
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

object LoadCSV extends LazyLogging {

  def handleLoadTable(db: Database, targetTable: String, sourceFile: File): Unit =
    handleLoadTable(db, targetTable, sourceFile, x => true)

  def handleLoadTable(db: Database, targetTable: String, sourceFile: File, detectHeader: String => Boolean){
    val input = new BufferedReader(new FileReader(sourceFile))
    val firstLine = input.readLine()
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
        if(detectHeader(firstLine)) {
          populateTable(db, input, targetTable, sch) // Ignore header since table already exists
        } else {
          populateTable(db, parser, targetTable, sch) // Ignore header since table already exists
        }

      case None =>
        val firstLine = ""
        if(largeData) { // if the data is large we may need to sacrafice error checking for speed
          if(detectHeader(firstLine)) {
            db.backend.update("CREATE TABLE "+targetTable+"("+
                makeColumnNamesUnique(input).map(_+" varchar").
                mkString(", ") +
              ")")
            handleLoadTable(db, targetTable, sourceFile)
          } else {
            throw new SQLException("No header supplied for creating new table")
          }
        } else { // first time around get the max number of columns and make sure it matches up to the number of columns in the schema
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

  private def sanitizeColumnName(name: String): String =
  {
    name.
      replace("^([0-9])","X\1").        // Prefix leading digits with an 'X'
      replaceAll("[^a-zA-Z0-9]+", "_"). // Replace sequences of non-alphanumeric characters with underscores
      replaceAll("_+$", "").            // Strip trailing underscores
      toUpperCase                       // Capitalize
  }

  private def makeColumnNamesUnique(src: BufferedReader): BufferedReader = {
    val headerLine: String = src.readLine()

    val originalHeaders: Array[String] = headerLine.split(',')
    var modifiedHeaders = new Array[String](0)

    for (originalHeader <- originalHeaders) {
      val cleanedOriginalHeader = sanitizeColumnName(originalHeader)

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

  private def populateTable(db: Database,
                            parser: CSVParser,
                            targetTable: String,
                            sch: List[(String, Type)]): Unit = {

    var location = 0
    var numberOfColumns = 0
    val keys = sch.map(_._1).map((x) => {numberOfColumns+= 1; "\'"+x+"\'"}).mkString(", ")
    val statements = new ListBuffer[String]()


    for (row: CSVRecord <- parser.asScala) {
        {
          var columnCount = 0

          var listOfValues: List[String] = row.iterator().asScala.toList

          listOfValues = listOfValues.take(numberOfColumns)
          listOfValues = listOfValues.padTo(numberOfColumns, null)

          val data = listOfValues.zip(sch).map({ 
              case ("", _) => "null"
              case (x, (_, (Type.TDate | Type.TString))) => "\'"+x.replaceAll("\\","\\\\").replaceAll("'","\\'")+"\'"
              case (x, (_, Type.TInt))   if x.matches("^[+-]?[0-9]+$") => x
              case (x, (_, Type.TFloat)) if x.matches("^[+-]?[0-9]+([.][0-9]+)?(e[+-]?[0-9]+)?$") => x
              case (x, (c,t)) => logger.warn(s"Don't know how to deal with $c ($t): $x, using null instead"); null
          }).mkString(", ")

          val cmd = "INSERT INTO " + targetTable + "(" + keys + ") VALUES (" + data.mkString(", ") + ")")
          logger.debug(s"INSERT: $cmd")
          db.backend.update(cmd)
        }
    }

  }
}
