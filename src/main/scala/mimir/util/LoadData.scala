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

object LoadData extends StrictLogging {
  
  def handleLoadTableRaw(db: Database, targetTable: Name, sourceFile: URL, options: Map[String,String] = Map(), format:Name = Name("csv")){
    //we need to handle making data publicly accessible here and adding it to spark for remote connections
    val path = if(sourceFile.getPath.contains(":/")) sourceFile.getPath.replaceFirst(":/", "://") else sourceFile.getAbsolutePath
    db.backend.readDataSource(targetTable, format, options, db.tableSchema(targetTable), Some(path)) 
  }

  def handleLoadTableRaw(db: Database, targetTable: Name, targetSchema:Option[Seq[(Name,Type)]], sourceFile: URL, options: Map[String,String], format:Name){
    val schema = targetSchema match {
      case None => db.tableSchema(targetTable)
      case _ => targetSchema
    }
    //we need to handle making data publicly accessible here and adding it to spark for remote connections
    val path = if(sourceFile.getPath.startsWith("jdbc:")) None else if(sourceFile.getPath.contains(":/")) Some(sourceFile.getPath.replaceFirst(":/", "://")) else Some(sourceFile.getAbsolutePath)
    db.backend.readDataSource(targetTable, format, options, schema, path) 
  }
}