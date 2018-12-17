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
 

  def handleLoadTableRaw(db: Database, targetTable: String, sourceFile: File, options: Map[String,String] = Map()){
    //we need to handle making csv publicly accessible here and adding it to spark for remote connections
    val path = if(sourceFile.getPath.contains(":/")) sourceFile.getPath.replaceFirst(":/", "://") else sourceFile.getAbsolutePath
    db.backend.readDataSource(targetTable, "csv", options, db.tableBaseSchema(targetTable), Some(path)) 
  }

  def handleLoadTableRaw(db: Database, targetTable: String, targetSchema:Option[Seq[(String,BaseType)]], sourceFile: File, options: Map[String,String]){
    val schema = targetSchema match {
      case None => db.tableBaseSchema(targetTable)
      case _ => targetSchema
    }
    //we need to handle making csv publicly accessible here and adding it to spark for remote connections
    val path = if(sourceFile.getPath.contains(":/")) sourceFile.getPath.replaceFirst(":/", "://") else sourceFile.getAbsolutePath
    db.backend.readDataSource(targetTable, "csv", options, schema, Some(path)) 
  }

}
