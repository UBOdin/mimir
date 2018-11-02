package mimir.models;

import java.io.File
import java.sql.SQLException
import java.util

import scala.collection.JavaConversions._
import scala.util._
import scala.util.Random

import com.typesafe.scalalogging.slf4j.Logger
import org.joda.time.{DateTime, Seconds, Days, Period}
import com.typesafe.scalalogging.slf4j.Logger

import mimir.algebra._
import mimir.ctables._
import mimir.util.{RandUtils,TextUtils,TimeUtils}
import mimir.Database
import mimir.models._
import mimir.util._
import mimir.statistics.DetectSeries
import org.apache.spark.sql.Dataset

import org.apache.spark.sql.functions.{col, monotonically_increasing_id, lit, not, isnull,asc,desc}  
import org.apache.spark.sql.DataFrame
import mimir.algebra.spark.OperatorTranslation
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row

//Upgrade: Move the series column detection to SeriesMissingValueModel

object SeriesMissingValueModel
{
  val logger = Logger(org.slf4j.LoggerFactory.getLogger("mimir.models.SeriesMissingValueModel"))

  def train(db: Database, name: String, columns: Seq[String], query:Operator): Map[String,(Model,Int,Seq[Expression])] = 
  {
    logger.debug(s"Train on: $query")
    val model = new SimpleSeriesModel(name, columns, query)
    val usefulColumns = model.train(db)
    columns.zip(usefulColumns)
      .zipWithIndex
      .filter(_._1._2)
      .map { case ((column, _), idx) => 
        (column -> (model, idx, Seq()))
      }
      .toMap
  }
}

case class SeriesColumnItem(columnName: String, reason: String, score: Double)

/** 
 * A model performs estimation of missing value column based on the column that follows a series.
 * Best Guess : Performs the best guess based on the weighted-average of upper and lower bound values.
 * Sample : Picks a random value within the range of uppper and lower bound
 * Train : Performs best guess on missing value fields. For each missing value, a map in created
 * 				 [ ROWID -> (Best Guess Value, Lower Bound Value, Upper Bound Value, Recieved Feedback) ] 
 *
 *  */

@SerialVersionUID(1000L)
class SimpleSeriesModel(name: String, colNames: Seq[String], query: Operator) 
  extends Model(name) 
  with SourcedFeedback
  with ModelCache
{
  
  var predictions:Seq[Dataset[(String, Double)]] = Seq()
  var queryDf: DataFrame = null
  var rowIdType:Type = TString()
  var dateType:Type = TDate()
    //colNames.map { _ => Seq[(String,Double)]() }
  var queryCols:Seq[String] = colNames
  var querySchema:Seq[(String,Type)] = 
    colNames.map { x => (x, TAny()) }

  def getCacheKey(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue] ) : String = s"${idx}_${args(0).asString}_${hints(0).asString}"
  def getFeedbackKey(idx: Int, args: Seq[PrimitiveValue] ) : String = s"${idx}_${args(0).asString}"

  def train(db: Database): Seq[Boolean] = 
  {
    querySchema = db.typechecker.schemaOf(query)
    queryCols = querySchema.unzip._1
    queryDf = db.backend.execute(query)
    rowIdType = db.backend.rowIdType
    dateType = db.backend.dateType
    
    predictions = 
      colNames.zipWithIndex.map { case (col, idx) => 
        DetectSeries.bestMatchSeriesColumn(
          db,
          col, 
          querySchema(idx)._2, 
          query
        )
      }

    SeriesMissingValueModel.logger.debug(s"Trained: $predictions")
    predictions.map(!_.limit(1).collect().isEmpty)
  }

  def validFor: Set[String] =
  {
    colNames
      .zip(predictions)
      .flatMap { 
        case (col, Seq()) => None
        case (col, _) => Some(col)
      }
      .toSet
  }
 
  
  def interpolate(idx: Int, args:Seq[PrimitiveValue], series: String): PrimitiveValue =
    interpolate(idx, args(0).asInstanceOf[RowIdPrimitive], series)
  def interpolate(idx: Int, rowid:RowIdPrimitive, series: String): PrimitiveValue =
  {
    val sp2m : (String, Row) => PrimitiveValue = (colName, row) => SparkUtils.convertField(querySchema.toMap.get(colName).get, row, row.fieldIndex(colName), dateType)
    val m2sp : PrimitiveValue => Any = prim => OperatorTranslation.mimirPrimitiveToSparkExternalRowValue(prim)
    val sprowid = m2sp(rowid)
    val rowIdVar = (monotonically_increasing_id()+1).alias(RowIdVar().toString()).cast(OperatorTranslation.getSparkType(rowIdType))
    val key = sp2m(series,queryDf.filter(rowIdVar === sprowid ).select(series).limit(1).collect().headOption.getOrElse(null))
    SeriesMissingValueModel.logger.debug(s"Interpolate $rowid with key $key")
    if(key == NullPrimitive()){ return NullPrimitive(); }
    val low = 
      queryDf.filter((col(series) <= lit(m2sp(key))).and(not(isnull(col(colNames(idx))))).and(rowIdVar =!= sprowid))
          .sort(desc(series))
          .limit(1)
          .select(series, colNames(idx)).collect().map( row => (sp2m(series,row), sp2m(colNames(idx),row)) ).toSeq
    val high = 
      queryDf.filter((col(series) >= lit(m2sp(key))).and(not(isnull(col(colNames(idx))))).and(rowIdVar =!= sprowid))
          .sort(desc(series))
          .limit(1)
          .select(series, colNames(idx)).collect().map( row => (sp2m(series,row), sp2m(colNames(idx),row)) ).toSeq

    SeriesMissingValueModel.logger.debug(s"   -> low = $low; high = $high")
   //println(low)
   //println(high)
    val result = (low, high) match {
      case (Seq(), Seq()) => NullPrimitive()
      case (Seq((_, low_v)), Seq())  => low_v
      case (Seq(), Seq((_, high_v))) => high_v
      case (Seq((low_k, low_v)), Seq((high_k, high_v))) => {
        val ratio = DetectSeries.ratio(low_k, key, high_k)
        SeriesMissingValueModel.logger.debug(s"   -> ratio = $ratio")
        DetectSeries.interpolate(low_v, ratio, high_v)
      }
    }
    setCache(idx, Seq(rowid), Seq(StringPrimitive(series)),result)
    result
  }

  def bestSequence(idx: Int): String = {
    val df = predictions(idx)
    df.sort(asc(df.columns(1))).limit(1).head._1
  }

  def argTypes(idx: Int) = Seq(TRowId())

  def varType(idx: Int, args: Seq[Type]): Type = querySchema(idx)._2 
  
  def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]  ): PrimitiveValue = 
  {
    getFeedback(idx, args) match {
      case Some(v) => v
      case None => {
        val bestSeries = bestSequence(idx)
        getCache(idx, args, Seq(StringPrimitive(bestSeries))) match {
          case Some(v) => v
          case None => {
            //throw new Exception(s"The Model is not trained: ${this.name}: idx: $idx  args: [${args.mkString(",")}] series: $bestSeries" )
            interpolate(idx, args, bestSeries)
          }
        }
      }
    }
  }
  
  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): PrimitiveValue = 
  {
    getFeedback(idx, args) match {
      case Some(v) => v
      case None => {
        //TODO: this should probably be scaled by variance.  For now... just pick entirely at random
        //val df = predictions(idx)
        val series = predictions(idx).sample(false, 0.1, randomness.nextInt()).limit(1).head()._1
        getCache(idx, args, Seq(StringPrimitive(series))) match {
          case Some(v) => v
          case None => {
            //throw new Exception(s"The Model is not trained: ${this.name}: idx: $idx  args: [${args.mkString(",")}] series: $series" )
            interpolate(idx, args, series)
          }
        }
        
      }
    }
  }

  def reason(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): String = {
    getFeedback(idx, args) match {
      case Some(value) =>
        s"You told me that $name.${colNames(idx)} = ${value} on row ${args(0)} (${args(0).getType})"
      case None => {
        val bestSeries = bestSequence(idx)
        getCache(idx, args, Seq(StringPrimitive(bestSeries))) match {
          case Some(value) => 
            s"I interpolated $name.${colNames(idx)}, ordered by $name.${bestSequence(idx)} to get $value for row ${args(0)}"
          case None =>{
            //s"I interpolated $name.${colNames(idx)}, ordered by $name.${bestSequence(idx)} row ${args(0)}"
            s"I interpolated $name.${colNames(idx)}, ordered by $name.${bestSeries} to get ${interpolate(idx, args, bestSeries)} for row ${args(0)}"
          }
        }
      }
    }
  }
  
  def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit = 
  {
    SeriesMissingValueModel.logger.debug(s"Feedback: $idx / $args (${args(0).getType}) <- $v")
    setFeedback(idx, args, v)
    SeriesMissingValueModel.logger.debug(s"Now: ${getFeedback(idx, args)}")
  }
  def isAcknowledged(idx: Int, args: Seq[PrimitiveValue]): Boolean =
    hasFeedback(idx, args)
  
  def hintTypes(idx: Int): Seq[mimir.algebra.Type] = Seq()
  
  def confidence (idx: Int, args: Seq[PrimitiveValue], hints:Seq[PrimitiveValue]) : Double = {
    val df = predictions(idx)
    df.sort(asc(df.columns(1))).limit(1).head._2
  }

}