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

object LoadJDBC extends StrictLogging {
 
  /**
   * Provide Options to load data from a jdbc source
   *
   * ex: Map("url" -> "jdbc:mysql://128.205.71.102:3306/mimirdb", 
            "driver" -> "com.mysql.jdbc.Driver", 
            "dbtable" -> "mimir_spark", 
            "user" -> "mimir", 
            "password" -> "mimir01")
   * 
   */
  def handleLoadTableRaw(db: Database, targetTable: String, options: Map[String,String]){
    db.backend.readDataSource(targetTable, "jdbc", options, None, None) 
  }
}
