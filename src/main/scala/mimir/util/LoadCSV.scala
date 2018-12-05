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
import mimir.models.DetectHeader

object LoadCSV extends StrictLogging {

  def handleLoadTable(db: Database, targetTable: String, sourceFile: File, options: Map[String,String] = Map()){
    db.loadTable(targetTable, sourceFile, true, ("CSV", options.toSeq.flatMap{
      case ("DELIMITER", delim) => Some(StringPrimitive(delim))
      case _ => None
    }))
  }
 
  def handleLoadTableRaw(db: Database, targetTable: String, sourceFile: File, options: Map[String,String] = Map()) = 
    LoadData.handleLoadTableRaw(db, targetTable, sourceFile, options)

  def handleLoadTableRaw(db: Database, targetTable: String, targetSchema:Option[Seq[(String,Type)]], sourceFile: File, options: Map[String,String]) =
    LoadData.handleLoadTableRaw(db, targetTable, targetSchema, sourceFile, options, "csv")
}


