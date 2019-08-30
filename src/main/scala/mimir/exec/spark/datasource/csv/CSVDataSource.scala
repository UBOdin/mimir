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

class DefaultSource extends DataSourceV2 with ReadSupport {

  def createReader(options: DataSourceOptions) = {
    val path = options.get("path").get
    new CSVDataSourceReader(path, options.asMap().asScala.toMap)
  }
}

class CSVDataSourceReader(path: String, options: Map[String,String]) extends DataSourceReader {

  def readSchema() = {
    val sparkContext = SparkSession.builder.getOrCreate().sparkContext
    val firstLine = sparkContext.textFile(path).first()
    val columnNames = firstLine.split(",")
    val structFields = columnNames.map(value ⇒ StructField(value, StringType))
    StructType(structFields)
  }

  def planInputPartitions: JList[InputPartition[InternalRow]] = {
    val sparkContext = SparkSession.builder.getOrCreate().sparkContext
    val rdd = sparkContext.textFile(path)
    List[InputPartition[InternalRow]](
    (0 to rdd.getNumPartitions - 1).map(value =>
      new CSVDataSourceReaderFactory(value, path, options)):_*).asJava
  }

}


class CSVDataSourceReaderFactory(partitionNumber: Int, filePath: String, options: Map[String,String], hasHeader: Boolean = true) 
extends InputPartition[InternalRow] 
with InputPartitionReader[InternalRow] {

  def createPartitionReader:InputPartitionReader[InternalRow] = new CSVDataSourceReaderFactory(partitionNumber, filePath, options, hasHeader)

  var row: Array[String] = null
  var parser: CsvParser = null
  
  @transient
  def next = {
    if (parser == null) {
      val sparkContext = SparkSession.builder.getOrCreate().sparkContext
      val out = new StringBuilder()
      val settings = new CsvParserSettings()
      settings.getFormat.setLineSeparator("\n")
      parser = new CsvParser(settings)
      parser.beginParsing(new FileReader(filePath))
    }
    row = parser.parseNext()
    row != null
  }

  def get = {
    InternalRow.fromSeq(row.toSeq.map(rte => UTF8String.fromString(rte)))
  }
  def close() = parser.stopParsing()
}