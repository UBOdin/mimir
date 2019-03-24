package mimir.util

import com.typesafe.scalalogging.slf4j.StrictLogging

import java.io.{File, FileReader, BufferedReader}
import java.io._
import java.nio.charset.StandardCharsets
import java.sql.SQLException
import java.util

import mimir.Database
import mimir.algebra.{Type,ID}
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.apache.commons.io.IOUtils
import org.apache.commons.io.input.ReaderInputStream
import scala.collection.JavaConverters._
import mimir.algebra._

object LoadData extends StrictLogging {
  
  def handleLoadTableRaw(
    db: Database, 
    targetTable: ID, 
    sourceFile: String, 
    targetSchema:Option[Seq[(ID,Type)]] = None,
    options: Map[String,String] = Map(), 
    format:ID = ID("csv")
  ){

    val schema = (targetSchema match {
      case None => db.tableSchema(targetTable)
      case _ => targetSchema
    })
    //we need to handle making data publicly accessible here and adding it to spark for remote connections
    val path = 
      if(sourceFile.startsWith("jdbc:")) { None }
      else if(sourceFile.contains(":/")) { Some(sourceFile.replaceFirst(":/", "://")) }
      else                               { Some(new File(sourceFile).getAbsolutePath) }
    db.backend.readDataSource(
      targetTable.id, 
      format.id, 
      options, 
      schema , 
      path
    ) 
  }
}