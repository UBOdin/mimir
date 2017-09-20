package mimir.util

import com.typesafe.scalalogging.slf4j.StrictLogging
import util.control.Breaks._

import java.io.{File, FileReader, BufferedReader}
import java.io._
import java.nio.charset.StandardCharsets
import java.sql.SQLException
import java.util

import mimir.parser._
import net.sf.jsqlparser.statement.Statement
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

  def SAMPLE_SIZE = 1000

  // def handleLoadTable(db: Database, targetTable: String, sourceFile: File): Unit =
  //   handleLoadTable(db, targetTable, sourceFile, Map())

  def handleLoadTable(db: Database, targetTable: String, sourceFile: File, options: Map[String,String] = Map()){
    val input = new FileReader(sourceFile)
    val in2 = new FileReader(sourceFile)

    /* Check if header present in CSV*/
    val par = new NonStrictCSVParser(in2, options);
    val assumeHeader = check_header(par.take(5))


    //val assumeHeader = options.getOrElse("HEADER", "YES").equals("YES")

    // Allocate the parser, and make its iterator scala-friendly
    val parser = new NonStrictCSVParser(input, options)



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
      db.tableSchema(targetTable) match {
        case Some(sch) => sch
        case None => {

          val idxToCol: Map[Int, String] =
            header.zipWithIndex.map { x =>
              if(x._1.equals("")){ (x._2, s"COLUMN_${x._2}")}
              else { (x._2, x._1) }
            }.toMap

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

    populateTable(db, samples.view++parser, targetTable, sourceFile, targetSchema)
    input.close()

      if (!assumeHeader){

        var len = 0;
        var arrs : Seq[mimir.algebra.PrimitiveValue] = null

        var str=""
        db.query("SELECT * FROM " + targetTable + " limit 1 ;")(_.foreach{result =>
          arrs =  result.tuple
        })
        len = arrs.length
        var flag = 0;

        for(i<- 0 until len){
          val res = arrs(i)
          var ch =  res.toString()(1)
          if(ch.toByte >= '0' && ch.toByte <= '9'){
            str ++= s"""COLUMN_$i AS COL_$i ,""";
          }
          else{
            str ++= s"""COLUMN_$i AS $res ,""";
          }
        }

        var query = ""
        println(str)

        str = str.slice(0,(str.length()-2));
        str = str.replaceAll("\\'","");
        query = "CREATE VIEW "+ targetTable +"_Header AS SELECT " +str +" from "+ targetTable + " limit 1,1000000000000"

        println(query)
        val stream = new ByteArrayInputStream(query.getBytes)
        var parser2 = new MimirJSqlParser(stream);
        val stmt: Statement = parser2.Statement();
        db.update(stmt);
      }


    }


  def check_header(sample: Iterator[MimirCSVRecord]): Boolean ={
    if (sample.hasNext){
      var header = sample.next.fields;
      val columnLength = header.size;
      var columnType =  scala.collection.mutable.Map[Int, String]()
      for(i <- 0 until columnLength ){
        columnType+= (i -> null)
      }
      var checked = 0
      var flag = 0;
      while(sample.hasNext){
        flag = 1;
        val row = sample.next.fields
        if(checked > 5){
          break
        }
        checked +=1
        /*if (row.size != columnLength){
          continue
        }*/
        for (col <- columnType.keySet){
          try{

            val i  = (row(col)).toFloat
            if(columnType(col) != "true"){
              if(columnType(col) == null){
                columnType(col) = "true"
              }
              else{
                columnType -= col
              }
            }
          }
          catch{
            case e: Exception =>{
              columnType(col)  = row(col).length().toString
            }
          }
        }
      }

      if (flag == 0){
        return false;
      }
      var hasHeader = 0
      for (c<-columnType.keySet){
        if(columnType(c)!="true"){
          if (header(c).length == columnType(c).toInt) {
              hasHeader=hasHeader-1;
          } else {
              hasHeader=hasHeader+1;
          }

        }else {
          try{
            header(c).toFloat
            hasHeader=hasHeader - 1;
          } catch {
              case e: Exception =>{
                hasHeader=hasHeader+1;
            }
          }

        }
      }
      return hasHeader > 0
    }

  return false;
  }

  def sanitizeColumnName(name: String): String =
  {
    logger.trace(s"SANITIZE: $name")
    name.
      replaceAll("^([0-9])","COLUMN_\1").  // Prefix leading digits with a 'COL_'
      replaceAll("[^a-zA-Z0-9]+", "_").    // Replace sequences of non-alphanumeric characters with underscores
      replaceAll("_+$", "").               // Strip trailing underscores
      replaceAll("^_+", "").               // Strip leading underscores
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
                            rows: Iterable[MimirCSVRecord],
                            targetTable: String,
                            sourceFile: File,
                            sch: Seq[(String, Type)]): Unit = {

    var location = 0
    val keys = sch.map(_._1).map((x) => "\'"+x+"\'").mkString(", ")
    var numberOfColumns = keys.size

    val cmd = "INSERT INTO " + targetTable + "(" + keys + ") VALUES (" + sch.map(x=>"?").mkString(",") + ")"

    logger.trace("BEGIN IMPORT")
    TimeUtils.monitor(s"Import CSV: $targetTable <- $sourceFile", logger.info(_)){
      db.backend.fastUpdateBatch(cmd, rows.view.map({ record =>
        if(record.recordNumber % 100000 == 0){
          logger.info(s"Loaded ${record.recordNumber} records...")
        }
        val data = record.fields.
          take(numberOfColumns).
          padTo(numberOfColumns, "").
          map( _.trim ).
          zip(sch).
          map({ case (value, (col, t)) =>
            if(value == null || value.equals("")) { NullPrimitive() }
            else {
              if(!Type.matches(Type.rootType(t), value))
              {
                logger.warn(s"$sourceFile:${record.lineNumber}: $col ($t) on is unparseable '$value', using null instead");
                NullPrimitive()
              } else {
                TextUtils.parsePrimitive(t, value)
              }
            }
          })

        logger.trace(s"INSERT (line ${record.lineNumber}): $cmd \n <- $data")
        record.fields = null
        data
      }))
    }

  }
}

case class MimirCSVRecord(var fields: Seq[String], lineNumber: Long, recordNumber: Long, comment: Option[String])

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
class NonStrictCSVParser(in:Reader, options: Map[String,String] = Map())
  extends Iterator[MimirCSVRecord]
  with StrictLogging
{
  var format = CSVFormat.DEFAULT.withAllowMissingColumnNames()
  options.get("DELIMITER") match {
    case None => ()
    case Some(delim) => {
      logger.debug(s"Using Delimiter ${delim.charAt(0)}")
      format = format.withDelimiter(delim.charAt(0))
    }
  }
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
