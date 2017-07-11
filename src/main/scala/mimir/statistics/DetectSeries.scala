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
class DetectSeries(db: Database) { 
  
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
//    Not working! Typechecker.schemaOf is not detecting type for csv loaded files( detected as 'Any')
//    val seqCol = Typechecker.schemaOf(oper) 
    val seqCol = db.bestGuessSchema(oper)
    
    val seriesColTemp = seqCol.flatMap( tup => getType(tup._1, tup._2))
    
    seriesColTemp.toList
  }

}