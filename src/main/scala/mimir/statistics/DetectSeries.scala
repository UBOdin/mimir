package mimir.statistics

import java.sql.SQLException
import java.sql.Date
import java.sql.Timestamp

import scala.collection.mutable.ArrayBuffer
import scala.collection._
import org.joda.time.{DateTime, Seconds, Days, Period, Duration}

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{min, max, sum, mean, stddev, col, lead, lag, abs, lit, isnull, not, desc, unix_timestamp, datediff, floor}       
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions.{desc, asc, col}

import mimir.Database
import mimir.algebra._
import mimir.ctables._
import mimir.parser._
import mimir.sql._
import mimir.util._
import mimir.exec.spark.RAToSpark

    

case class ColumnStepStatistics(
  name: ID,
  t: Type,
  meanStep: PrimitiveValue,
  relativeStepStddev: Double,
  low: PrimitiveValue,
  high: PrimitiveValue
)

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
  def VALID_SEQUENCE_TYPES = Set[Type](
    TInt(),
    TFloat(),
    TDate(),
    TTimestamp()
  )

  def seriesOf(
    db: Database,
    query: Operator
  ): Seq[ColumnStepStatistics] = 
    seriesOf(
      db.compiler.compileToSparkWithRewrites(query),
      db.typechecker.schemaOf(query)
    )

  def seriesOf(
    queryDf:DataFrame, 
    querySchema:Seq[(ID, Type)]
  ): Seq[ColumnStepStatistics] = {

    val candidateColumns: Seq[(ID, Type)] = 
      querySchema.filter { VALID_SEQUENCE_TYPES contains _._2 }
    
    candidateColumns.map { case (column, t) =>
      gatherStatistics(queryDf, column, t)
    }
  }

  def gatherStatistics(
    db: Database,
    query: Operator,
    column: ID
  ):ColumnStepStatistics = 
    gatherStatistics(
      db.compiler.compileToSparkWithRewrites(query),
      column,
      db.typechecker.typeOf(Var(column), query)
    )

  def gatherStatistics(
    queryDf: DataFrame,
    column: ID,
    t: Type
  ):ColumnStepStatistics = 
  {
    val windowSpec = 
      Window.partitionBy(lit(0))
            .orderBy(col("curr"))  
    
    val results = 
      queryDf.select(column.id)
             .withColumnRenamed(column.id, "curr")
             .withColumn("prev", lag(col("curr"), 1)
                                    .over(windowSpec))
             .withColumn("diff", col("curr") - col("prev"))
             .agg(
                mean("diff").alias("step_mean"),
                stddev("diff").alias("step_stddev"),
                min("prev").alias("low"),
                max("curr").alias("high")
              )
             .withColumn("step_relstddev", col("step_stddev") / col("step_mean"))
             .head

    val getDelta: (String => PrimitiveValue) =
      t match {
        case TInt() => 
          { (v:String) => IntPrimitive(results.getAs[Long](v)) }
        case TFloat() => 
          { (v:String) => FloatPrimitive(results.getAs[Long](v)) }
        case TDate() | TTimestamp() => 
          { (v:String) => IntervalPrimitive(results.getAs[Period](v)) }
        case _ => 
          throw new SQLException(s"Unsupported sequence column type $t")
      }
    val getValue: (String => PrimitiveValue) =
      t match {
        case TInt() => 
          { (v:String) => IntPrimitive(results.getAs[Long](v)) }
        case TFloat() => 
          { (v:String) => FloatPrimitive(results.getAs[Long](v)) }
        case TDate() => 
          { (v:String) => SparkUtils.convertDate(results.getAs[Date](v)) }
        case TTimestamp() => 
          { (v:String) => SparkUtils.convertTimestamp(results.getAs[Timestamp](v)) }
        case _ => 
          throw new SQLException(s"Unsupported sequence column type $t")
      }

    ColumnStepStatistics(
      column,
      t,
      getDelta("step_mean"),
      results.getAs[Double]("step_relstddev"),
      getValue("low"),
      getValue("high")
    )
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
  def makeSeries(
    low: PrimitiveValue, 
    high: PrimitiveValue, 
    step: PrimitiveValue
  ): Seq[PrimitiveValue] =
  {
    val lowIsBiggerThanHigh = Eval.applyCmp(Cmp.Gt, low, high).asBool
    val stepIsNegative = Eval.applyCmp(Cmp.Gt, low,
                            Eval.applyArith(Arith.Add, low, step)).asBool
    val (start, end) = 
      if((lowIsBiggerThanHigh || stepIsNegative) 
          && (lowIsBiggerThanHigh != stepIsNegative)){
        // flip if low is bigger than high, OR if stepIsNegative, but not both
        (high, low)
      } else { 
        (low, high) 
      }
    val terminalCmp = if(stepIsNegative){ Cmp.Gte } else { Cmp.Lte }

    return new Iterator[PrimitiveValue] {
      var curr = start
      def hasNext = Eval.applyCmp(terminalCmp, curr, end).asBool
      def next: PrimitiveValue = {
        val ret = curr
        curr = Eval.applyArith(Arith.Add, curr, step)
        return ret
      }
    }.toSeq
  }
}