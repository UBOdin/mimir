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


//Define the data type of Series columns
//case class SeriesColumnItem(columnName: String, reason: CellExplanation)
case class SeriesColumnItem(columnName: String, reason: String, score: Double)

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
  def seriesOf(db: Database, query: Operator, threshold: Double): Seq[SeriesColumnItem] = {

    // Hack for Type Inference lens
    val queryColumns = db.typechecker.schemaOf(query)
    
    // Date columns are automatically series
    val seriesColumnDate = queryColumns.flatMap( tup => 
      tup._2 match{
        case TDate() | TTimestamp() =>  Some(SeriesColumnItem(tup._1, "The column is a "+tup._2.toString()+" datatype.", 1.0))
        case _ => None
      }
    )

    var series = seriesColumnDate.toSeq
    
    // Numeric columns *might* be series
    // Currently test for the case where the inter-query spacing is (mostly) constant.
    // i.e., 1, 2, 3, 4, 5.1, 5.9, ...
    val seriesColumnNumeric: Seq[(String, Type)] = queryColumns.filter(tup => Seq(TInt(),TFloat()).contains(tup._2)).toSeq
    
    // For each column A generate a query of the form:
    // SELECT A FROM Q(...) ORDER BY A
    
    seriesColumnNumeric
      .map { x => query.sort((x._1, true)).project(x._1) }
      .zipWithIndex
      .foreach { case (testQuery, idx) => 

        var diffAdj: Seq[Double] = Seq()
        var sum: Double = 0
        var count = 0

        // Run the query
        db.query(testQuery) { result =>

          val rowWindow = 
            result
              .map { _(0) } // Only one attribute in each row.  Pick it out
              .sliding(2)   // Get a 2-element sliding window over the result
          for( rowPair <- rowWindow ){
            val a = rowPair(0)
            val b = if(rowPair.length > 1) rowPair(1) else NullPrimitive()
            if(!a.equals(NullPrimitive()) && !b.equals(NullPrimitive())){
              val currDiff = a.asDouble - b.asDouble
              sum += currDiff
              diffAdj = diffAdj :+ (currDiff)
              count += 1
            }
          }
        }
        val mean = Math.floor((sum/count)*10000)/10000
        val stdDev = Math.floor(Math.sqrt((diffAdj.map(x => (x-mean)*(x-mean)).sum)/count)*10000)/10000
        val relativeStdDev = Math.abs(stdDev/mean)
        if(relativeStdDev < threshold) {
          series = series :+ (
            SeriesColumnItem(
              seriesColumnNumeric(idx)._1, 
              "The column is a Numeric ("+seriesColumnNumeric(idx)._2.toString()+") datatype with an effective increasing pattern.", 
              if((1-relativeStdDev)<0){ 0 } else { 1-relativeStdDev }
            )
          )
        }
      }
    series
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
  
  
  def bestMatchSeriesColumn(db: Database, colName: String, colType: Type, query: Operator): Seq[(String,Double)] = 
    bestMatchSeriesColumn(db, colName, colType, query, seriesOf(db, query, 0.2))
  def bestMatchSeriesColumn(db: Database, colName: String, colType: Type, query: Operator, seriesColumnList: Seq[SeriesColumnItem]): Seq[(String,Double)] = {
    
    seriesColumnList
      .filter( seriesCol => seriesCol.columnName != colName ) // Make sure we don't match the column itself
      .flatMap { seriesCol => 
        // To test out a particular column, we use a query of the form
        // SELECT colName FROM Q(...) ORDER BY seriesCol
        // We're expecting to see a relatively good interpolation
        val projectQuery = 
          query
            .sort((seriesCol.columnName,true))
            .filter( Var(seriesCol.columnName).isNull.not )
            .project(seriesCol.columnName, colName)
        
        db.query(projectQuery) { result =>
          val rowWindow = 
            result
              .map { _.tuple }
              .sliding(3)

          var sumError:Double   = 0.0
          var sumErrorSq:Double = 0.0
          var sumTot:Double     = 0.0
          
          val (actual, error) =
            rowWindow.flatMap { triple =>
              val key_low  = triple(0)(0); val v_low  = triple(0)(1)
              val key_test = triple(1)(0); val v_test = triple(1)(1)
              val key_high = triple(2)(0); val v_high = triple(2)(1)

              if( triple.flatten.contains(NullPrimitive()) ){
                None
              } else {
                val scale = ratio(key_low, key_test, key_high)
                val v_predicted = interpolate(v_low, scale, v_high)
                Some(v_test, Eval.applyAbs(Eval.applyArith(Arith.Sub, v_test, v_predicted)))
              }
            }.toSeq.unzip

          if(actual.isEmpty){
            None     // If we have nothing to base a decision on... this can't be a particularly good predictor
          } else {
            // Otherwise, average everything out...
            val actualSum = 
              actual.tail.fold(actual.head)(Eval.applyArith(Arith.Add, _, _))
            val errorSum = 
              error.tail.fold(error.head)(Eval.applyArith(Arith.Add, _, _))
           
            (errorSum, actualSum) match {
              case (_, IntervalPrimitive(_)) => None
              case (_, DatePrimitive(_,_,_)) => None
              case (IntervalPrimitive(_), _) => None
              case (DatePrimitive(_,_,_), _) => None // because in Eval: case (Arith.Div, TInterval(), _, _) => 
                                                                          //throw new RAException("Division not quite yet supported")
              case _ => Eval.applyArith(Arith.Div, errorSum, actualSum) match {
                case NullPrimitive() => None
                case n:NumericPrimitive => Some((seriesCol.columnName, n.asDouble))
                case _ => throw new RAException("Eval should return a numeric on series detection")
              }
            }
          }
        }
      }

  }



}