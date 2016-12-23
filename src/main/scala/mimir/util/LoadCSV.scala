package mimir.util

import com.typesafe.scalalogging.slf4j.StrictLogging

import java.io.{File, FileReader, BufferedReader}
import java.io._
import java.nio.charset.StandardCharsets
import java.sql.SQLException
import java.util

import mimir.Database
import mimir.algebra.Type
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.apache.commons.io.IOUtils
import org.apache.commons.io.input.ReaderInputStream
import scala.collection.JavaConverters._
import mimir.algebra._

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.Breaks._

object LoadCSV extends StrictLogging {

  def SAMPLE_SIZE = 10000

  def handleLoadTable(db: Database, targetTable: String, sourceFile: File): Unit =
    handleLoadTable(db, targetTable, sourceFile, true)

  def handleLoadTable(db: Database, targetTable: String, sourceFile: File, assumeHeader: Boolean){
    val input = new FileReader(sourceFile)

    // Allocate the parser, and make its iterator scala-friendly
    val parser = new NonStrictCSVParser(input)

    // Pull out the header if appropriate
    val header: Seq[String] = 
      if(assumeHeader && parser.hasNext){ parser.decrementRecordCount; parser.next.fields }
      else { Nil }

    // Grab some sample data for testing 
    //(note the toSeq being used to materialize the samples)
    val samples = parser.take(SAMPLE_SIZE).toSeq

    // Produce a schema --- either one already exists, or we need
    // to generate one.
    val targetSchema = 
      db.getTableSchema(targetTable) match {
        case Some(sch) => sch
        case None => {

          val idxToCol: Map[Int, String] = 
            header.zipWithIndex.map( x => (x._2, x._1) ).toMap

          logger.debug(s"HEADER_MAP: $idxToCol")

          val columnCount = 
            (samples.map( _.fields.size ) ++ List(0)).max

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
      filter( x => (x.fields.size > targetSchema.size) ).
      map( _.lineNumber ).
      toList match {
        case Nil          => // All's well! 
        case a::Nil       => logger.warn(s"Too many fields on line $a of $sourceFile")
        case a::b::Nil    => logger.warn(s"Too many fields on lines $a and $b of $sourceFile")
        case a::b::c::Nil => logger.warn(s"Too many fields on lines $a, $b, and $c of $sourceFile")
        case a::b::rest   => logger.warn(s"Too many fields on lines $a, $b, and "+(rest.size)+s" more of $sourceFile")
      }

    populateTable(db, samples++parser, targetTable, sourceFile, targetSchema)
    input.close()
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
                            rows: TraversableOnce[MimirCSVRecord],
                            targetTable: String,
                            sourceFile: File,
                            sch: Seq[(String, Type)]): Unit = {

    var location = 0
    val keys = sch.map(_._1).map((x) => "\'"+x+"\'").mkString(", ")
    var numberOfColumns = keys.size

    val cmd = "INSERT INTO " + targetTable + "(" + keys + ") VALUES (" + sch.map(x=>"?").mkString(",") + ")"

    logger.trace("BEGIN IMPORT")
    TimeUtils.monitor(s"Import CSV: $targetTable <- $sourceFile",
      () => {
        db.backend.fastUpdateBatch(cmd, rows.map({ record => 
          val data = record.fields.
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
                  logger.warn(s"fileName:${record.lineNumber}: $col ($t) on is unparseable '$value', using null instead");
                  NullPrimitive()
                } else {
                  TextUtils.parsePrimitive(t, value)
                }
              }
            })

          logger.trace(s"INSERT (line ${record.lineNumber}): $cmd \n <- $data")
          data
        }))
      },
      logger.info(_)
    )

  }
}

case class MimirCSVRecord(fields: Seq[String], lineNumber: Long, recordNumber: Long, comment: Option[String])

/**
 * A wrapper around the Apache Commons CSVParser that can recover from malformed data.
 *
 * Recovery is, at present, rather dumb.  CSVParser begins parsing the record anew
 * from the point where the malformed data appeared.
 * 
 * It would be nice if we could retain the record prefix that has already been parsed
 * (as well as offsetting data).  Unfortunately, these changes all require changes to
 * CSVParser.getNextRecord(), which relies on private access to Token and Lexer.  
 * 
 * Suggested approaches:
 *  - Submit a push request to commons with a "Recovery" callback
 *  - Swap out CSVParser with a different off-the-shelf parser (e.g., Spark has a few)
 *  - Write our own CSVParser.
 */
class NonStrictCSVParser(in:Reader)
  extends Iterator[MimirCSVRecord]
  with StrictLogging
{
  val format = CSVFormat.DEFAULT.withAllowMissingColumnNames()
  val parser = new CSVParser(in, format)
  val iter = parser.iterator.asScala
  var record: Option[(Seq[String], Option[String])] = None
  var recordOffset = 0;

  def bufferNextRecord(): Unit = 
  {
    while(record == None){
      try {
        // iter.hasNext needs to take place inside the try/catch block, 
        // since it pre-buffers another line of data.
        if(!iter.hasNext){ return; }

        // Pull out the next record
        val curr = iter.next

        // There's a comment field
        val comment = 
          curr.getComment match { case null => None; case x => Some(x) }

        // And pull out the record itself
        record = Some(curr.asScala.toIndexedSeq, comment)
      } catch {
        case e: RuntimeException => {
          e.getCause() match {
            case t: IOException => 
              logger.warn(s"Parse Error: ${t.getMessage}")
            case _ => 
              throw new IOException("Parsing error", e)
          }
        }
      }
    }
  }

  def hasNext(): Boolean =
  {
    bufferNextRecord()
    return record != None
  }

  def next(): MimirCSVRecord =
  {
    bufferNextRecord()
    val (fields, comment) = record.get
    logger.trace(s"READ: $fields")
    record = None
    return MimirCSVRecord(fields, parser.getCurrentLineNumber, parser.getRecordNumber + recordOffset, comment)
  }

  def decrementRecordCount(): Unit = 
    { recordOffset -= 1; }

}
