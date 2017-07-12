package mimir.statistics

import mimir.Database
import mimir.algebra._
import mimir.ctables._
import mimir.parser._
import mimir.sql._
import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.select.{FromItem, PlainSelect, Select, SelectBody} 
import net.sf.jsqlparser.statement.drop.Drop

import scala.collection.mutable.ArrayBuffer
import scala.collection._


//Define the data type of Series columns
//case class SeriesColumnItem(columnName: String, reason: CellExplanation)
case class SeriesColumnItem(columnName: String, reason: String, score: Double)

/* DetectSeries
 * DetectSeries is a series in a dataset where, when you sort on a given column
 * (call it the order column), adjacent rows will be related.
 * There are various methods to detect series:
 * 	-> Checking Types (Date and TimeStamp)
 *  -> Linear Regression
 *  -> Column Names
 *  -> Markov Predictor
 */
class DetectSeries(db: Database, threshold: Double) { 
  
  //Stores the list of series corresponding to each table
  val tabSeqMap = collection.mutable.Map[String, Seq[SeriesColumnItem]]()
  
  def detectSeriesOf(sel: Select): Seq[SeriesColumnItem] = {
    
    val queryOperator = db.sql.convert(sel)
    detectSeriesOf(queryOperator)
  }
  def detectSeriesOf(oper: Operator): Seq[SeriesColumnItem] = {

    val queryColumns = db.bestGuessSchema(oper)
    
    val seriesColumnDate = queryColumns.flatMap( tup => 
      tup._2 match{
        case TDate() | TTimestamp() =>  Some(SeriesColumnItem(tup._1, "The column is a "+tup._2.toString()+" datatype.", 1.0))
        case _ => None
      }
    )

    var series = seriesColumnDate.toSeq
    
    val seriesColumnNumeric: Seq[(String, Type)] = queryColumns.filter(tup => Seq(TInt(),TFloat()).contains(tup._2)).toSeq
    
    val queryList = seriesColumnNumeric.map(x => oper.sort((x._1, true)).project(x._1))
    
    queryList.zipWithIndex.foreach(x => db.query(x._1) {result =>
      val rowWindow = result.sliding(2)
      var diffAdj: Seq[Double] = Seq()
      var sum: Double = 0
      var count = 0

      while(rowWindow.hasNext){
        val rowPair = rowWindow.next
        if(!db.interpreter.evalBool(rowPair(1).tuple(0).isNull.or(rowPair(0).tuple(0).isNull))){
          val currDiff = rowPair(1).tuple(0).asDouble - rowPair(0).tuple(0).asDouble
          sum += currDiff
          diffAdj = diffAdj :+ (currDiff)
          count += 1
        }
      }
      val mean = Math.floor((sum/count)*10000)/10000
      val stdDev = Math.floor(Math.sqrt((diffAdj.map(x => (x-mean)*(x-mean)).sum)/count)*10000)/10000
      val relativeStdDev = Math.abs(stdDev/mean)
      if(relativeStdDev < threshold)
        series = series :+ (SeriesColumnItem(seriesColumnNumeric(x._2)._1, "The column is a Numeric("+seriesColumnNumeric(x._2)._2.toString()+") datatype with an effective increasing pattern.", if((1-relativeStdDev)<0) 0 else 1-relativeStdDev))
    })
    series
  }

}