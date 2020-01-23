package mimir.exec.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ DataType, LongType }
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{
  spark_partition_id,
  monotonically_increasing_id,
  count,
  sum,
  first,
  lit,
  col
}

object MimirSparkRuntimeUtils
{
  def zipWithIndex(df: DataFrame, offset: Long = 1, indexName: String = "ROWIDX", indexType:DataType = LongType): DataFrame = {
    val dfWithPartitionId = df.withColumn("partition_id", spark_partition_id()).withColumn("inc_id", monotonically_increasing_id())

    val partitionOffsets = dfWithPartitionId
        .groupBy("partition_id")
        .agg(count(lit(1)) as "cnt", first("inc_id") as "inc_id")
        .orderBy("partition_id")
        .select(col("partition_id"), sum("cnt").over(Window.orderBy("partition_id")) - col("cnt") - col("inc_id") + lit(offset) as "cnt" )
        .collect()
        .map(row => (row.getInt(0), row.getLong(1)))
        .toMap

     val theUdf = org.apache.spark.sql.functions.udf(
       (partitionId: Int) => partitionOffsets(partitionId), 
       LongType
     )
     
     dfWithPartitionId
        .withColumn("partition_offset", theUdf(col("partition_id")))
        .withColumn(indexName, (col("partition_offset") + col("inc_id")).cast(indexType))
        .drop("partition_id", "partition_offset", "inc_id")
  }

  def writeDataSink(dataframe:DataFrame, format:String, options:Map[String, String], save:Option[String]) = {
    val dsFormat = dataframe.write.format(format) 
    val dsOptions = options.toSeq.foldLeft(dsFormat)( (ds, opt) => opt._1 match { 
      case "mode" => ds.mode(opt._2) 
      case _ => ds.option(opt._1, opt._2)
      })
    save match {
      case None => dsOptions.save
      case Some(outputFile) => {
        if(format.equals("com.github.potix2.spark.google.spreadsheets")){
          val gsldfparts = outputFile.split("\\/") 
          val gsldf = s"${gsldfparts(gsldfparts.length-2)}/${gsldfparts(gsldfparts.length-1)}"
          dsOptions.save(gsldf)
        }
        else{
          dsOptions.save(outputFile)
        }
      }
    }
  }
}