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

  def SAMPLE_SIZE = 10000

  def handleLoadTable(db: Database, targetTable: String, sourceFile: File): Unit =
    handleLoadTable(db, targetTable, sourceFile, true)

  def handleLoadTable(db: Database, targetTable: String, sourceFile: File, assumeHeader: Boolean){
    val largeData = false
    val input = new BufferedReader(new FileReader(sourceFile))
    var config = CSVFormat.DEFAULT.withAllowMissingColumnNames()

    if(assumeHeader){ config = config.withHeader() }

    val parser = new CSVParser(input, config)
    val samples = parser.asScala.take(SAMPLE_SIZE)

    val targetSchema = 
      db.getTableSchema(targetTable) match {
        case Some(sch) => sch
        case None => {

          val idxToCol: Map[Int, String] = 
            if(parser.getHeaderMap != null) { 
              parser.getHeaderMap.
                entrySet.asScala.
                map( (x:java.util.Map.Entry[String,Integer]) => (x.getValue.toInt, x.getKey) ).
                toMap
            } else { Map() }

          logger.debug(s"HEADER_MAP: $idxToCol")

          val columnCount = 
            samples.map( _.size ).max

          val columnNames =
            makeColumnNamesUnique(
              (0 until columnCount).
                map( (idx:Int) => { idxToCol.getOrElse(idx, s"COLUMN_$idx") } ).
                map( sanitizeColumnName _ )
            )

          val columnsDDL = columnNames.map(_+" varchar").mkString(", ")

          val createTableStatement = s"""
            CREATE TABLE $targetTable($columnsDDL)
          """
          logger.debug(s"INIT: $createTableStatement")
          db.backend.update(createTableStatement)

          columnNames.map( (_, TString()) )
        }
      }

    // Sanity check the size of each row
    samples.
      filter( x => (x.size > targetSchema.size) ).
      map( _.getRecordNumber ) match {
        case Nil          => // All's well! 
        case a::Nil       => logger.warn(s"Too many fields on line $a of $sourceFile")
        case a::b::Nil    => logger.warn(s"Too many fields on lines $a and $b of $sourceFile")
        case a::b::c::Nil => logger.warn(s"Too many fields on lines $a, $b, and $c of $sourceFile")
        case a::b::rest   => logger.warn(s"Too many fields on lines $a, $b, and "+(rest.size)+s" more of $sourceFile")
      }

    populateTable(db, samples++parser.asScala, targetTable, sourceFile, targetSchema)
  }

  def sanitizeColumnName(name: String): String =
  {
    logger.trace(s"SANITIZE: $name")
    name.
      replaceAll("^([0-9])","COLUMN_\1").  // Prefix leading digits with a 'COL_'
      replaceAll("[^a-zA-Z0-9]+", "_").    // Replace sequences of non-alphanumeric characters with underscores
      replaceAll("_+$", "").               // Strip trailing underscores
      toUpperCase                          // Capitalize
  }

  def makeColumnNamesUnique(columnNames: Iterable[String]): List[String] = {
    val dupColumns = 
      columnNames.
        toList.
        groupBy( x => x ).
        filter( _._2.length > 1 ).
        map(_._1).
        toSet

    val uniqueColumnNames =
      if(dupColumns.isEmpty){ columnNames }
      else {
        var idx = scala.collection.mutable.Map[String,Int]()
        idx ++= dupColumns.map( (_, 0) ).toMap

        columnNames.map( x =>
          if(idx contains x){ idx(x) += 1; x+"_"+idx(x) }
          else { x }
        )
      }

    return uniqueColumnNames.toList
  }

  private def populateTable(db: Database,
                            rows: TraversableOnce[CSVRecord],
                            targetTable: String,
                            sourceFile: File,
                            sch: List[(String, Type)]): Unit = {

    var location = 0
    var numberOfColumns = 0
    val keys = sch.map(_._1).map((x) => {numberOfColumns+= 1; "\'"+x+"\'"}).mkString(", ")
    val statements = new ListBuffer[String]()


    for (row: CSVRecord <- rows) {
        {
          var listOfValues = row.iterator().asScala.toList
          val lineNum = row.getRecordNumber()

          val data = listOfValues.
            take(numberOfColumns).
            padTo(numberOfColumns, "").
            map( _.trim ).
            zip(sch).
            map({ case (value, (col, t)) =>
              if(value == null || value.equals("")) { NullPrimitive() }
              else {
                if(Type.tests.contains(t) 
                    && !value.matches(Type.tests(t)))
                {
                  logger.warn(s"fileName:$lineNum: $col ($t) on is unparseable '$value', using null instead");
                  NullPrimitive()
                } else {
                  TextUtils.parsePrimitive(t, value)
                }
              }
            })

          val cmd = "INSERT INTO " + targetTable + "(" + keys + ") VALUES (" + data.map(x=>"?").mkString(",") + ")"
          logger.trace(s"INSERT: $cmd \n <- $data")
          db.backend.update(cmd, data)
        }
    }

  }
}
