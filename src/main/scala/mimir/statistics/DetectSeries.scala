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
  val tabSeqMap = collection.mutable.Map[String, List[SeriesColumnItem]]()
  
  def detectSeriesOf(sel: Select): List[SeriesColumnItem] = {
    
    val queryOperator = db.sql.convert(sel)
    detectSeriesOf(queryOperator)
  }
  def detectSeriesOf(oper: Operator): List[SeriesColumnItem] = {

    def getType(col: String, colType: Type) = {
      colType match{
        case TDate() | TTimestamp() =>  Some(SeriesColumnItem(col, "The column is a "+colType.toString()+" datatype.", 1.0))
        case _ => None
      }
    }
    val seqCol = db.bestGuessSchema(oper)
    
    val seriesColDate = seqCol.flatMap( tup => getType(tup._1, tup._2))

    var series = seriesColDate.toList
    
    val seriesColNumIdx: Seq[Int] = seqCol.zipWithIndex.filter(tup => Seq(TInt(),TFloat()).contains(tup._1._2)).map(x => x._2).toSeq
    
    if(!seriesColNumIdx.isEmpty){
      db.query(oper){result =>
//        var prevVal = new ArrayBuffer[PrimitiveValue](seriesColNumIdx.size)
        val initRow = result.next
        var prevVal = seriesColNumIdx.map(initRow.tuple(_)).toArray
        var countInc = new Array[Double](seriesColNumIdx.size)
        var countDec = new Array[Double](seriesColNumIdx.size)
        var totalTup = 1;
        result.foreach(row => 
          { totalTup = totalTup+1;
            seriesColNumIdx.zipWithIndex.foreach(x => 
              if(db.interpreter.evalBool(prevVal(x._2).lt(row.tuple(x._1))))
                { prevVal(x._2)=row.tuple(x._1); countInc(x._2) = countInc(x._2)+1.0}
              else if(db.interpreter.evalBool(prevVal(x._2).eq(row.tuple(x._1)))){
                prevVal(x._2)=row.tuple(x._1); countInc(x._2) = countInc(x._2)+1.0; countDec(x._2) = countDec(x._2)+1.0;
              }
              else
                { 
                  if(!db.interpreter.evalBool(row.tuple(x._1).isNull.or(prevVal(x._2).isNull)))
                  {countDec(x._2) = countDec(x._2)+1.0}
                  prevVal(x._2)=row.tuple(x._1);
                 })
          })
        val seriesIncProb: Seq[Double] = countInc.map(x => (x.toDouble/(totalTup-1).toDouble))
        val seriesDecProb: Seq[Double] = countDec.map(x => (x.toDouble/(totalTup-1).toDouble))
        val numIncSeries: Seq[SeriesColumnItem] = seriesIncProb.zipWithIndex.filter(x => x._1 > threshold).map(x => SeriesColumnItem(seqCol(seriesColNumIdx(x._2))._1, "The column is a Numeric("+seqCol(seriesColNumIdx(x._2))._2.toString()+") datatype with high imcremental ordering.", x._1))
        val numDecSeries: Seq[SeriesColumnItem] = seriesDecProb.zipWithIndex.filter(x => x._1 > threshold).map(x => SeriesColumnItem(seqCol(seriesColNumIdx(x._2))._1, "The column is a Numeric("+seqCol(seriesColNumIdx(x._2))._2.toString()+") datatype with high decremental ordering.", x._1))

        series = series ++ numIncSeries.toList ++ numDecSeries.toList
      }
    }    

    series
  }

}