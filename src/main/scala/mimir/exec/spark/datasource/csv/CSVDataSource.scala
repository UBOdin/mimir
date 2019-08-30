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

class SerializableConfiguration(@transient var value: Configuration) extends Serializable {
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
    value.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit =  {  
    value = new Configuration(false)
    value.readFields(in)
  }
}

class CSVDataSourceReaderFactory(partitionNumber: Int, filePath: String, options: Map[String,String], hasHeader: Boolean = true) 
extends InputPartition[InternalRow] 
with InputPartitionReader[InternalRow] {

  def createPartitionReader:InputPartitionReader[InternalRow] = new CSVDataSourceReaderFactory(partitionNumber, filePath, options, hasHeader)

  var iterator: Iterator[InternalRow] = null

  @transient
  def next = {
    if (iterator == null) {
      val sparkSession = MimirSpark.get.sparkSession//SparkSession.builder.getOrCreate()
      val sparkContext = sparkSession.sparkContext
      val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast( new SerializableConfiguration(sparkContext.hadoopConfiguration))
      val parsedOptions = new CSVOptions(
      options,
      sparkSession.sessionState.conf.csvColumnPruning,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)
      val caseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
      val columnPruning = sparkSession.sessionState.conf.csvColumnPruning
      val conf = broadcastedHadoopConf.value.value
      val requiredSchema = StructType(Seq())
      val fileS = new FileStatus()
      fileS.setPath(new Path(Paths.get(filePath).toFile().toURI()))
      val mCsvParser = MimirCSVDataSource(parsedOptions)
      val dataSchema = mCsvParser.inferSchema(sparkSession, Seq(fileS), parsedOptions).getOrElse(StructType(Seq()))
      val parser = new UnivocityParser(
        StructType(dataSchema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord)),
        StructType(requiredSchema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord)),
        parsedOptions)
      
      iterator = mCsvParser.readFile(
        conf,
        PartitionedFile(InternalRow.fromSeq(Seq(partitionNumber)),
          filePath,
          0,
          100),
        parser,
        requiredSchema,
        dataSchema,
        caseSensitive,
        columnPruning)
    }
    iterator.hasNext
  }

  def get = {
    iterator.next()
  }
  def close() = Unit
}