
package mimir.exec.spark.datasource.csv

import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources.v2.reader._
import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import java.util.{Collections, List => JList, Optional}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.execution.datasources.ubodin.csv.MimirCSVDataSource
import org.apache.spark.sql.execution.datasources.ubodin.csv.CSVOptions
import org.apache.spark.sql.execution.datasources.ubodin.csv.UnivocityParser
import org.apache.hadoop.fs.FileStatus

import java.nio.file.Paths
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import java.io.ObjectOutputStream
import java.io.ObjectInputStream
import org.apache.spark.util.Utils
import org.apache.spark.sql.execution.datasources.PartitionedFile
import mimir.exec.spark.MimirSpark
import com.univocity.parsers.csv.CsvParserSettings
import com.univocity.parsers.csv.CsvParser
import java.io.Reader
import java.io.InputStreamReader
import java.io.FileReader
//import scala.util.parsing.input.Reader
import scala.util.parsing.input.Position
import java.io.{ByteArrayInputStream, InputStream, SequenceInputStream}
import mimir.util.StringUtils
import org.apache.spark.rdd.RDD

class DefaultSource extends DataSourceV2 with ReadSupport {

  def createReader(options: DataSourceOptions) = {
    val path = options.get("path").get
    new CSVDataSourceReader(path, options.asMap().asScala.toMap)
  }
}


class CSVDataSourceReader(path: String, options: Map[String,String]) extends DataSourceReader {

  var colCount = 0
  var rdd:RDD[String] = null
  
  def getColCount():Int = {
    if(colCount == 0){
      val sparkContext = SparkSession.builder.getOrCreate().sparkContext
      rdd = sparkContext.textFile(path)
      val top = rdd.take(10)
      if(top.isEmpty)
        throw new Exception("Error: the csv datasource is empty.")
      val delimeter = options.getOrElse("delimeter", ",")
      colCount = top.foldLeft(Map(1 ->1)) {
        case (init, curr) => {
           val currCount = StringUtils.countSubstring(curr, delimeter) + 1
           if(currCount > 0)
             init.updated(currCount, init.getOrElse(currCount, 0) + 1)
           else init
        }
      }.maxBy(_._2)._1
      colCount
    }
    else
      colCount
  }
  
  def readSchema() : StructType = {
    val structFields = (1 to getColCount()).map(value ⇒ StructField(s"_c$value", StringType))
    StructType(structFields)
  }

  def planInputPartitions: JList[InputPartition[InternalRow]] = {
    val colCount = getColCount()
    List[InputPartition[InternalRow]](
    (0 to rdd.getNumPartitions - 1).map(value =>
      new CSVDataSourceReaderFactory(value, path, options, colCount)):_*).asJava
  }

}



class CSVDataSourceReaderFactory(partitionNumber: Int, filePath: String, options: Map[String,String], val colCount:Int, hasHeader: Boolean = true) 
extends InputPartition[InternalRow] 
with InputPartitionReader[InternalRow] {

  def createPartitionReader:InputPartitionReader[InternalRow] = new CSVDataSourceReaderFactory(partitionNumber, filePath, options, colCount, hasHeader)

  var row: Array[String] = null
  var parser: CsvParser = null
  var iterator: Iterator[String] = null
  val parsedOptions = new CSVOptions(
      options,
      SparkSession.builder.getOrCreate().sessionState.conf.csvColumnPruning,
      SparkSession.builder.getOrCreate().sessionState.conf.sessionLocalTimeZone,
      SparkSession.builder.getOrCreate().sessionState.conf.columnNameOfCorruptRecord)
  
  @transient
  def next = {
    if (iterator == null) {
      val sparkContext = SparkSession.builder.getOrCreate().sparkContext
      val out = new StringBuilder()
      val settings = parsedOptions.asParserSettings
      val delimeter = options.getOrElse("delimeter", ",").charAt(0)
      settings.getFormat.setDelimiter(delimeter)
      settings.setHeaderExtractionEnabled(false)
      parser = new CsvParser(settings)
      val rdd = sparkContext.textFile(filePath)
      val partition = rdd.partitions(partitionNumber)
      iterator = rdd.iterator(partition, org.apache.spark.TaskContext.get())
    }
    if(iterator.hasNext){
      try{
        val rowStr = iterator.next()
        //parser.beginParsing(new ByteArrayInputStream(rowStr.getBytes("UTF-8")))
        row = parser.parseLine(rowStr)//parseNext()
        //parser.stopParsing()
      } catch {
        case t:Throwable => {
          row = ((1 to colCount).map(el => "")).toArray
        }
      }
      true
    }
    else false
  }

  def get = {
    val cols = (1 to colCount)
    val fullRow = cols.zipAll(row,-1,"")
    InternalRow.fromSeq(fullRow.map(rte => {
      UTF8String.fromString(rte._2)}))
  }
  def close() = parser.stopParsing()
}