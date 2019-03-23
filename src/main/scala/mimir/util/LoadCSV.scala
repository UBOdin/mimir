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

  def handleLoadTable(
    db: Database, 
    targetTable: ID, 
    sourceFile: File, 
    options: Map[String,String] = Map()
  ){
    db.loadTable(
      sourceFile, 
      targetTable = Some(targetTable), 
      force = true, 
      format = ID("csv"),
      loadOptions = options
    )
  }
 
  def handleLoadTableRaw(
    db: Database, 
    targetTable: ID, 
    sourceFile: File, 
    targetSchema:Option[Seq[(ID,Type)]] = None,
    options: Map[String,String] = Map()
  ) = 
    LoadData.handleLoadTableRaw(
      db, 
      targetTable = targetTable, 
      sourceFile = sourceFile, 
      targetSchema = targetSchema,
      options = options,
      format = ID("csv")
    )
}


