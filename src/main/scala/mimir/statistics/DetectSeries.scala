package mimir.statistics

import mimir.Database
import mimir.algebra._
import mimir.ctables._
import mimir.parser._
import mimir.sql._
import mimir.util._
import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.select.{FromItem, PlainSelect, Select, SelectBody} 
import net.sf.jsqlparser.statement.drop.Drop
import org.joda.time.{DateTime, Seconds, Days, Period, Duration}

import scala.collection.mutable.ArrayBuffer
import scala.collection._
import org.apache.spark.sql.expressions.Window
import mimir.algebra.spark.OperatorTranslation
import org.apache.spark.sql.functions.{sum, mean, stddev, col, lead, lag, abs, lit, isnull, not, desc, unix_timestamp}       
import org.apache.spark.sql.Encoders
import mimir.models.SeriesColumnItem
import org.apache.spark.sql.Dataset




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
  def seriesOf(db: Database, query: Operator, threshold: Double): Dataset[SeriesColumnItem] = {

    // Hack for Type Inference lens
    val queryColumns = db.typechecker.schemaOf(query)
    
    // Date columns are automatically series
    val seriesColumnDate = queryColumns.flatMap( tup => 
      tup._2 match{
        case TDate() | TTimestamp() =>  Some(SeriesColumnItem(tup._1, "The column is a "+tup._2.toString()+" datatype.", 1.0))
        case _ => None
      }
    )

    
    // Numeric columns *might* be series
    // Currently test for the case where the inter-query spacing is (mostly) constant.
    // i.e., 1, 2, 3, 4, 5.1, 5.9, ...
    val seriesColumnNumeric: Seq[(String, Type)] = queryColumns.filter(tup => Seq(TInt(),TFloat()).contains(tup._2)).toSeq
    
    val scnf = seriesColumnNumeric.map(tup => (tup._1, tup._2.toString()))
    val thresh = threshold
    
    val dataframes = seriesColumnNumeric
      .map { x => query.sort((x._1, true)).project(x._1) }
      .map { testQuery =>  db.backend.execute(testQuery) }
      (dataframes.zipWithIndex.foldLeft(db.backend.asInstanceOf[SparkBackend].sparkSql.createDataset(seriesColumnDate.toSeq)(org.apache.spark.sql.Encoders.kryo[SeriesColumnItem]))( (init, curr) => {
        val (dataframe, idx) = curr
        val rowWindowSpec = Window.orderBy(dataframe.columns(0))  
        val rowWindowDf = dataframe.withColumn("next", lag(dataframe.columns(0), 1).over(rowWindowSpec))
        val diffDf = rowWindowDf.withColumn("diff", rowWindowDf.col(rowWindowDf.columns(0))-rowWindowDf.col(rowWindowDf.columns(1)))
        //diffDf.show()
        val meanStdDevRelStddevDf = diffDf.agg(mean("diff").alias("mean"), stddev("diff").alias("stddev")).withColumn("relstddev", col("stddev") / col("mean"))
        //meanStdDevRelStddevDf.show()
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
        println("-----------------------------------------------ratio date")
        return new Duration(l, m).getMillis().toDouble / new Duration(l, h).getMillis().toDouble
      } 
      case ( lt, mt, ht ) =>
        throw new RAException(s"Can't get ratio for [ $lt - $mt - $ht ]")
    }
  }
  def interpolate(low: PrimitiveValue, mid: Double, high: PrimitiveValue): PrimitiveValue =
  {
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
        println("-----------------------------------------------interp date")
        
        DatePrimitive(ret.getYear, ret.getMonthOfYear, ret.getDayOfMonth)
      }
      case ( (TDate() | TTimestamp()), (TDate() | TTimestamp())) => {
        val l = low.asDateTime
        val h = high.asDateTime
        val ret = l.plus(new Duration(l, h).dividedBy( (1/mid).toLong ))
        println("-----------------------------------------------interp ts")
        
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
  
  
  def bestMatchSeriesColumn(db: Database, colName: String, colType: Type, query: Operator): Dataset[(String,Double)] = {
    //bestMatchSeriesColumn(db, colName, colType, query, seriesOf(db, query, 0.2))
       // Hack for Type Inference lens
    val queryColumns = db.typechecker.schemaOf(query)
    
    // Date columns are automatically series
    val seriesColudmnDate : Seq[(String, Type)] = queryColumns.filter(tup => Seq(TDate(),TTimestamp()).contains(tup._2)).toSeq

    
    // Numeric columns *might* be series
    // Currently test for the case where the inter-query spacing is (mostly) constant.
    // i.e., 1, 2, 3, 4, 5.1, 5.9, ...
    val seriesColumnNumeric: Seq[(String, Type)] = queryColumns.filter(tup => Seq(TInt(),TFloat()).contains(tup._2)).toSeq
    
    val scnf = (seriesColumnNumeric ++ seriesColudmnDate).map(tup => (tup._1, tup._2.toString()))
    val thresh = 0.2
    
    val queryDf = db.backend.execute(query)
    val dataframes = seriesColumnNumeric
      .map { x => query.sort((x._1, true)).project(x._1) }
      .map { testQuery => db.backend.execute(testQuery) } ++ seriesColudmnDate
      .map { x => query.sort((x._1, true)).project(x._1) }
      .map { testQuery => {
          val df = db.backend.execute(testQuery)
          df.withColumn("timestamp", unix_timestamp(df.col(df.columns(0))))
          .select(col("timestamp"))
          .withColumnRenamed("timestamp", df.columns(0)) 
      }}
      val retdf = (dataframes.zipWithIndex.foldLeft(db.backend.asInstanceOf[SparkBackend].sparkSql.createDataset(Seq())(Encoders.product[(String,Double)]))( (init, curr) => {
        val (dataframe, idx) = curr
        val rowWindowSpec = Window.orderBy(dataframe.columns(0))  
        val rowWindowDf = dataframe.withColumn("next", lag(dataframe.columns(0), 1).over(rowWindowSpec)).na.drop
        val diffDf = rowWindowDf.withColumn("diff", rowWindowDf.col(rowWindowDf.columns(0))-rowWindowDf.col(rowWindowDf.columns(1)))
        //diffDf.show()
        val meanStdDevRelStddevDf = diffDf.agg(mean("diff").alias("mean"), stddev("diff").alias("stddev")).withColumn("relstddev", col("stddev").divide( col("mean")))
        //meanStdDevRelStddevDf.show()
        val dfrow = meanStdDevRelStddevDf.head
        val relativeStdDev = dfrow.getDouble(2)
        val columnName = scnf(idx)._1
        if(relativeStdDev < thresh && columnName != colName /*Make sure we don't match the column itself*/) {
          val rowWindowSpeci = Window.orderBy(queryDf.col(columnName))  
          val rowWindowDfi = queryDf.sort(desc(columnName)).filter(not(isnull(col(columnName)))).select(col(columnName), col(colName))
                                  .withColumn("prev0", lag(col(columnName), 1).over(rowWindowSpeci))
                                  .withColumn("next0", lead(col(columnName), 1).over(rowWindowSpeci))
                                  .withColumn("prev1", lag(col(colName), 1).over(rowWindowSpeci))
                                  .withColumn("next1", lead(col(colName), 1).over(rowWindowSpeci))
                                  rowWindowDfi.show()
          val outDfi = rowWindowDfi.na.drop.withColumn("ratio", (col(columnName) - col("prev0")) / (col("next0") - col("prev0")))    
                                  .withColumn("interpolate", (col("next0") - col("prev0")) * col("ratio") + col("prev0"))
                                  .withColumn("actual", col(colName))
                                  .withColumn("error", abs((col(colName) - col("interpolate"))))
                                  .agg(sum("error").alias("error_sum"), sum("actual").alias("actual_sum"))
                                  .withColumn("result", col("error_sum").divide(col("actual_sum")))
                                  .select("result")
                                  .withColumn("sname", lit(colName))
          init.union( outDfi.map(row => (row.getString(1), row.getDouble(0)))(Encoders.product[(String,Double)]))  
        }
        else {
          println(s"skipped: $columnName == $colName || $relativeStdDev >= $thresh ")
          init
        }
      }))
      //retdf.show()
      retdf
      
  }
  
  /*def bestMatchSeriesColumn(db: Database, colName: String, colType: Type, query: Operator, seriesColumnList: Seq[SeriesColumnItem]): Dataset[(String,Double)] = {
    
    val dataframes = seriesColumnList
      .filter( seriesCol => seriesCol.columnName != colName ) // Make sure we don't match the column itself
      .map(seriesCol => {
        // To test out a particular column, we use a query of the form
        // SELECT colName FROM Q(...) ORDER BY seriesCol
        // We're expecting to see a relatively good interpolation
        val projectQuery = 
          query
            .sort((seriesCol.columnName,true))
            .filter( Var(seriesCol.columnName).isNull.not )
            .project(seriesCol.columnName, colName)
        
        db.backend.execute(projectQuery) 
      })
      dataframes.foldLeft(db.backend.asInstanceOf[SparkBackend].sparkSql.createDataset(Seq())(Encoders.product[(String, Double)]))( (init, dataframe) => {
        val rowWindowSpec = Window.orderBy(dataframe.col(colName))  
        val rowWindowDf = dataframe.withColumn("prev0", lag(dataframe.columns(0), 1).over(rowWindowSpec))
                                   .withColumn("next0", lead(dataframe.columns(0), 1).over(rowWindowSpec))
                                   .withColumn("prev1", lag(dataframe.columns(1), 1).over(rowWindowSpec))
                                   .withColumn("next1", lead(dataframe.columns(1), 1).over(rowWindowSpec))
        val outDf = rowWindowDf.na.drop.withColumn("ratio", (col(rowWindowDf.columns(0)) - col("prev0")) / (col("next0") - col("prev0")))    
                                    .withColumn("interpolate", (col("next0") - col("prev0")) * col("ratio") + col("prev0"))
                                    .withColumn("actual", col(rowWindowDf.columns(1)))
                                    .withColumn("error", abs((col(rowWindowDf.columns(1)) - col("interpolate"))))
                                    .agg((sum("error")/sum("actual")).alias("result"))
                                    .select("result")
                                    .withColumn("sname", lit(colName))
       init.union(outDf.map(row => (row.getString(1), row.getDouble(0)))(Encoders.product[(String, Double)]))
        
      })

  }*/



}