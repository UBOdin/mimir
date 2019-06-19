package mimir.statistics

import mimir.Database
import mimir.algebra._
import mimir.ctables._
import mimir.parser._
import mimir.sql._
import mimir.util._
import org.joda.time.{DateTime, Seconds, Days, Period, Duration}

import scala.collection.mutable.ArrayBuffer
import scala.collection._
import org.apache.spark.sql.expressions.Window
import mimir.algebra.spark.OperatorTranslation
import org.apache.spark.sql.functions.{sum, mean, stddev, col, lead, lag, abs, lit, isnull, not, desc, unix_timestamp, datediff, floor}       
import org.apache.spark.sql.Encoders
import mimir.models.SeriesColumnItem
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions.{desc, asc, col}
    



/** 
 *  DetectSeries
 * DetectSeries is a series in a dataset where, when you sort on a given column
 * (call it the order column), adjacent rows will be related.
 * 
 * 
 * 
 */
object DetectSeries
{ 
  def seriesOf(queryDf:DataFrame, querySchema:Seq[(ID, Type)], threshold: Double): Dataset[SeriesColumnItem] = {

    
    // Date columns are automatically series
    val seriesColumnDate = querySchema.flatMap( tup => 
      tup._2 match{
        case TDate() | TTimestamp() =>  Some(SeriesColumnItem(tup._1, "The column is a "+tup._2.toString()+" datatype.", 1.0))
        case _ => None
      }
    )

    
    // Numeric columns *might* be series
    // Currently test for the case where the inter-query spacing is (mostly) constant.
    // i.e., 1, 2, 3, 4, 5.1, 5.9, ...
    val seriesColumnNumeric: Seq[(ID, Type)] = querySchema.filter(tup => Seq(TInt(),TFloat()).contains(tup._2)).toSeq
    
    val scnf = seriesColumnNumeric.map(tup => (tup._1, tup._2.toString()))
    val thresh = threshold
    
    val dataframes = seriesColumnNumeric
      .map { x => queryDf.sort(asc(x._1.id)).select(x._1.id) }
      
      (dataframes.zipWithIndex.foldLeft(queryDf.sparkSession.createDataset(seriesColumnDate.toSeq)(org.apache.spark.sql.Encoders.kryo[SeriesColumnItem]))( (init, curr) => {
        val (dataframe, idx) = curr
        val rowWindowSpec = Window.partitionBy(lit(0)).orderBy(dataframe.columns(0))  
        val rowWindowDf = dataframe.withColumn("next", lag(dataframe.columns(0), 1).over(rowWindowSpec))
        val diffDf = rowWindowDf.withColumn("diff", rowWindowDf.col(rowWindowDf.columns(0))-rowWindowDf.col(rowWindowDf.columns(1)))
        val meanStdDevRelStddevDf = diffDf.agg(mean("diff").alias("mean"), stddev("diff").alias("stddev")).withColumn("relstddev", col("stddev") / col("mean"))
        init.union( meanStdDevRelStddevDf.flatMap( dfrow => {
          val relativeStdDev = dfrow.getDouble(2)
          if(relativeStdDev < thresh) {
            Some (
              SeriesColumnItem(
                scnf(idx)._1, 
                s"The column is a Numeric (${scnf(idx)._2}) datatype with an effective increasing pattern.", 
                if((1-relativeStdDev)<0){ 0 } else { 1-relativeStdDev }
              )
            )
          }
          else None
        })(org.apache.spark.sql.Encoders.kryo[SeriesColumnItem]))
      }))
  }

  def ratio(low: PrimitiveValue, mid: PrimitiveValue, high: PrimitiveValue): Double =
  {
    (low.getType, mid.getType, high.getType) match {
      case ( (TInt() | TFloat()), (TInt() | TFloat()), (TInt() | TFloat()) ) => {
        val l = low.asDouble
        val m = mid.asDouble
        val h = high.asDouble
        return (m - l) / (h - l)
      }
      case ( (TDate() | TTimestamp()),  (TDate() | TTimestamp()),  (TDate() | TTimestamp())) => {
        val l = low.asDateTime
        val m = mid.asDateTime
        val h = high.asDateTime
        return new Duration(l, m).getMillis().toDouble / new Duration(l, h).getMillis().toDouble
      } 
      case ( lt, mt, ht ) =>
        throw new RAException(s"Can't get ratio for [ $lt - $mt - $ht ]")
    }
  }
  def interpolate(low: PrimitiveValue, mid: Double, high: PrimitiveValue): PrimitiveValue =
  {
    //println(s"DetectSeries.interpolate: low: ${low.getClass.getName} mid:$mid high: ${high.getClass.getName}")
    (low.getType, high.getType) match {
      case ( TInt(), TInt() ) => {
        val l = low.asLong
        val h = high.asLong
        return IntPrimitive( ((h - l) * mid + l).toLong )
      }
      case ( (TInt() | TFloat()), (TInt() | TFloat()) ) => {
        val l = low.asDouble
        val h = high.asDouble
        return FloatPrimitive( (h - l) * mid + l )
      }
      case ( TDate(), TDate() ) => {
        val l = low.asDateTime
        val h = high.asDateTime
        val ret = l.plus(new Duration(l, h).dividedBy( (1/mid).toLong ))
        DatePrimitive(ret.getYear, ret.getMonthOfYear, ret.getDayOfMonth)
      }
      case ( (TDate() | TTimestamp()), (TDate() | TTimestamp())) => {
        val l = low.asDateTime
        val h = high.asDateTime
        val ret = l.plus(new Duration(l, h).dividedBy( (1/mid).toLong ))
        TimestampPrimitive(
          ret.getYear,
          ret.getMonthOfYear,
          ret.getDayOfMonth,
          ret.getHourOfDay,
          ret.getMinuteOfHour,
          ret.getSecondOfMinute,
          ret.getMillisOfSecond
        )
      }
      case ( lt, ht ) =>
        throw new RAException(s"Can't interpolate [ $lt - $ht ]")
    }
  }
  
  //def bestMatchSeriesColumn(colName: String, colType: Type, queryDf:DataFrame, querySchema:Seq[(String, Type)]): Dataset[(String,Double)] = 
  //  bestMatchSeriesColumn(colName, colType, queryDf, querySchema, seriesOf(queryDf, querySchema, 0.2))
  def bestMatchSeriesColumn(
    colName: ID, 
    colType: Type, 
    queryDf:DataFrame, 
    querySchema:Seq[(ID, Type)], 
    threshold:Double = 0.2
  ): Dataset[(ID,Double)] = {
    //bestMatchSeriesColumn(db, colName, colType, query, seriesOf(db, query, 0.2))
      
    // Date columns are automatically series
    val seriesColudmnDate : Seq[(ID, Type)] = querySchema.filter(tup => Seq(TDate(),TTimestamp()).contains(tup._2) && !tup._1.equals(colName)).toSeq

    
    // Numeric columns *might* be series
    // Currently test for the case where the inter-query spacing is (mostly) constant.
    // i.e., 1, 2, 3, 4, 5.1, 5.9, ...
    val seriesColumnNumeric: Seq[(ID, Type)] = querySchema.filter(tup => Seq(TInt(),TFloat()).contains(tup._2) && !tup._1.equals(colName)).toSeq
    
    val scnf = (seriesColumnNumeric ++ seriesColudmnDate).map(tup => (tup._1, tup._2.toString()))
    val dataframes = 
      seriesColumnNumeric.map { x => queryDf.sort(asc(x._1.id)).select(x._1.id) }  ++ 
      seriesColudmnDate.map { x => queryDf.sort(asc(x._1.id)).select(x._1.id) }
      
      dataframes.foreach(df => df.cache())
      
      (dataframes.zipWithIndex.foldLeft(queryDf.sparkSession.createDataset(Seq())(Encoders.product[(ID,Double)]))( (init, curr) => {
        val (dataframe, idx) = curr
        val seriesColumnName = scnf(idx)._1
        val curTypes = (querySchema.toMap.getOrElse(seriesColumnName,TInt()), colType)
        val relativeStdDev = if(idx < seriesColumnNumeric.length){
          val rowWindowSpec = Window.partitionBy(lit(0)).orderBy(dataframe.columns(0))  
          val rowWindowDf = dataframe.withColumn("next", lead(dataframe.columns(0), 1).over(rowWindowSpec)).na.drop
          val diffDf = rowWindowDf.withColumn("diff", rowWindowDf.col(rowWindowDf.columns(0))-rowWindowDf.col(rowWindowDf.columns(1)))
          val meanStdDevRelStddevDf = diffDf.agg(mean("diff").alias("mean"), stddev("diff").alias("stddev")).withColumn("relstddev", abs(col("stddev").divide( col("mean"))))
          val dfrow = meanStdDevRelStddevDf.head
          val relStdDevIdx = dfrow.fieldIndex("relstddev")
          if(dfrow.isNullAt(relStdDevIdx)) Double.MaxValue else dfrow.getDouble(relStdDevIdx)
        } else 0.0
        if(relativeStdDev < threshold  ) {
          val rowWindowSpeci = Window.partitionBy(lit(0)).orderBy(queryDf.col(seriesColumnName.id))  
          val rowWindowDfi = queryDf.sort(desc(seriesColumnName.id)).filter(not(isnull(col(seriesColumnName.id)))).select(col(seriesColumnName.id), col(colName.id))
                              .withColumn("prev0", lag(col(seriesColumnName.id), 1).over(rowWindowSpeci))
                              .withColumn("next0", lead(col(seriesColumnName.id), 1).over(rowWindowSpeci))
                              .withColumn("prev1", lag(col(colName.id), 1).over(rowWindowSpeci))
                              .withColumn("next1", lead(col(colName.id), 1).over(rowWindowSpeci))
          val scaleDf = curTypes match {
            //case (TDate(), _) => rowWindowDfi.na.drop.withColumn("ratio", datediff(col("prev0"), col(seriesColumnName)) / datediff(col("prev0"), col("next0")))
            case (TDate() | TTimestamp(),_) => {
              rowWindowDfi.na.drop.withColumn("ratio", ( col("prev0") - col(seriesColumnName.id)) / (col("prev0")- col("next0")))
            }
            case _ => rowWindowDfi.na.drop.withColumn("ratio", (col(seriesColumnName.id) - col("prev0")) / (col("next0") - col("prev0")))
          } 
          val predictedDf = curTypes match {
            //case (_, TDate() | TTimestamp()) => scaleDf.withColumn("interpolate", datediff(col("prev1"), col("next1")) / (lit(1.0)/col("ratio")))
            case (_, TTimestamp() | TDate()) => {
              scaleDf.withColumn("interpolate", col("prev1") + ((col("prev1")-col("next1")) / (lit(1.0)/col("ratio"))))
            }
            case _ => scaleDf.withColumn("interpolate", (col("next1") - col("prev1")) * col("ratio") + col("prev1"))                     
          }
          val outDfi = predictedDf.withColumn("actual", col(colName.id))
                          .withColumn("error", abs((col(colName.id) - col("interpolate"))))
                          .agg(sum("error").alias("error_sum"), sum("actual").alias("actual_sum"))               
                          .withColumn("result", (col("error_sum") / col("actual_sum")))
                          .select("result")
                          .withColumn("sname", lit(seriesColumnName.id))
          init.union( outDfi.na.drop.map(row => (ID(row.getString(1)), row.getDouble(0)))(Encoders.product[(ID,Double)]))  
        }
        else init
      }))    
  }
}