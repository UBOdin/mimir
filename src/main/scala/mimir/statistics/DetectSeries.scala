package mimir.statistics

import mimir.Database
import mimir.algebra._
import mimir.ctables._
import mimir.parser._
import mimir.sql._
import mimir.algebra.Type._
import mimir.util.{TimeUtils,ExperimentalOptions,LineReaderInputSource}
import mimir.exec.{OutputFormat,DefaultOutputFormat,PrettyOutputFormat}
import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.select.{FromItem, PlainSelect, Select, SelectBody} 
import net.sf.jsqlparser.statement.drop.Drop
import org.jline.terminal.{Terminal,TerminalBuilder}
import org.slf4j.{LoggerFactory}
import ch.qos.logback.classic.{Level, Logger}
import org.rogach.scallop._

import scala.collection.mutable.ArrayBuffer
import scala.collection._


//Define the data type of Series columns
//case class SeriesColumnItem(columnName: String, reason: CellExplanation)
case class SeriesColumnItem(columnName: String, reason: String)

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
  val tabSeqMap = collection.mutable.Map[String, ArrayBuffer[SeriesColumnItem]]()
  
  def detectSeriesOf(sel: Select): List[SeriesColumnItem] = {
    
    val queryOperator = db.sql.convert(sel)
    detectSeriesOf(queryOperator)
  }
  def detectSeriesOf(oper: Operator): List[SeriesColumnItem] = {

//    Not working! Typechecker.schemaOf is not detecting type for csv loaded files( detected as 'Any')
//    val seqCol = Typechecker.schemaOf(oper) 
    val seqCol = db.bestGuessSchema(oper)
    
    val seriesColTemp = ArrayBuffer[SeriesColumnItem]()
    
    for((col, colType) <- seqCol ){
      if(colType.toString() == "date" || colType.toString() == "datetime"){
        
        val candCol = new SeriesColumnItem(col, "The column is a "+colType.toString()+" datatype.")
        
        seriesColTemp += candCol;
      }
    }
    
    seriesColTemp.toList
  }
  
  def detectSeriesOf(datab: Database): Unit = {
    
    val tableSet = datab.getAllTables();
    
    for(tableName <- tableSet){
      val oprTable = datab.getTableOperator(tableName)
      val tabSchema = db.bestGuessSchema(oprTable)

      val seriesColTemp = ArrayBuffer[SeriesColumnItem]()
        for((col, colType) <- tabSchema){
          if(colType.toString() == "date" || colType.toString() == "datetime"){
        
            val candCol = new SeriesColumnItem(col, "The column is a "+colType.toString()+" datatype.")
        
            seriesColTemp += candCol;
          }
        }
        
        tabSeqMap += (tableName -> seriesColTemp)
    }
  }
  
  def getSeriesOf(tableName: String): List[SeriesColumnItem] = {
     tabSeqMap.getOrElse(tableName, ArrayBuffer()).toList
  }

}