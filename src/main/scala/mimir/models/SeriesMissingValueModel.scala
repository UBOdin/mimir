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
import mimir.{Analysis, Database}
import mimir.models._
import mimir.util._
import mimir.statistics.DetectSeries

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
        (column -> (model, idx, Seq(RowIdVar())))
      }
      .toMap
  }
}
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
  with NeedsReconnectToDatabase 
  with SourcedFeedback
  with ModelCache
{
  @transient var db: Database = null

  var predictions = 
    colNames.map { _ => Seq[(String,Double)]() }
  var querySchema:Seq[Type] = 
    colNames.map { _ => TAny() }

  def getCacheKey(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue] ) : String = args(0).asString
  def getFeedbackKey(idx: Int, args: Seq[PrimitiveValue] ) : String = args(0).asString

  def train(db: Database): Seq[Boolean] = 
  {
    this.db = db

    querySchema = db.bestGuessSchema(query).map { _._2 }
    
    val potentialSeries = DetectSeries.seriesOf(db, query, 0.1)
    predictions = 
      colNames.zipWithIndex.map { case (col, idx) => 
        DetectSeries.bestMatchSeriesColumn(
          db,
          col, 
          querySchema(idx), 
          query, 
          potentialSeries
        )
      }

    SeriesMissingValueModel.logger.debug(s"Trained: $predictions")
    predictions.map { !_.isEmpty }
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
  
  def reconnectToDatabase(db: Database): Unit = {
    this.db = db
  }

  def interpolate(idx: Int, args:Seq[PrimitiveValue], series: String): PrimitiveValue =
    interpolate(idx, args(0).asInstanceOf[RowIdPrimitive], series)
  def interpolate(idx: Int, rowid:RowIdPrimitive, series: String): PrimitiveValue =
  {
    val key = 
      db.query(
        query.filter(RowIdVar().eq(rowid)).project(series)
      ) { results => if(results.hasNext){ val t = results.next; t(0) } else { NullPrimitive() } }
    SeriesMissingValueModel.logger.debug(s"Interpolate $rowid with key $key")
    if(key.equals(NullPrimitive())){ return NullPrimitive(); }
    val low = 
      db.query(
        query
          .filter(Var(series).lte(key).and(Var(colNames(idx)).isNull.not).and(RowIdVar().neq(rowid)))
          .sort(series -> false)
          .limit(1)
          .project(series, colNames(idx))
      ) { results => if(results.hasNext){ val t = results.next.tuple; Some((t(0), t(1))) } else { None } }
    val high = 
      db.query(
        query
          .filter(Var(series).gte(key).and(Var(colNames(idx)).isNull.not.and(RowIdVar().neq(rowid))))
          .sort(series -> true)
          .limit(1)
          .project(series, colNames(idx))
      ) { results => if(results.hasNext){ val t = results.next.tuple; Some((t(0), t(1))) } else { None } }

    SeriesMissingValueModel.logger.debug(s"   -> low = $low; high = $high")

    (low, high) match {
      case (None, None) => NullPrimitive()
      case (Some((_, low_v)), None)  => low_v
      case (None, Some((_, high_v))) => high_v
      case (Some((low_k, low_v)), Some((high_k, high_v))) => {
        val ratio = DetectSeries.ratio(low_k, key, high_k)
        SeriesMissingValueModel.logger.debug(s"   -> ratio = $ratio")


        DetectSeries.interpolate(low_v, ratio, high_v)
      }
    }
  }

  def bestSequence(idx: Int): String = 
    predictions(idx).sortBy(_._2).head._1

  def argTypes(idx: Int) = Seq(TRowId())

  def varType(idx: Int, args: Seq[Type]): Type = querySchema(idx) 
  
  def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]  ): PrimitiveValue = 
  {
    getFeedback(idx, args) match {
      case Some(v) => v
      case None => {
        getCache(idx, args, hints) match {
          case Some(v) => v
          case None => {
            val v = interpolate(idx, args, bestSequence(idx))
            setCache(idx, args, hints, v)
            v
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
        // this should probably be scaled by variance.  For now... just pick entirely at random
        val series = RandUtils.pickFromList(randomness, predictions(idx).map { _._1 })
        return interpolate(idx, args, series)
      }
    }
  }

  def reason(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): String = {
    getFeedback(idx, args) match {
      case Some(value) =>
        s"You told me that $name.${colNames(idx)} = ${value} on row ${args(0)} (${args(0).getType})"
      case None => {
        getCache(idx, args, hints) match {
          case Some(value) => 
            s"I interpolated $name.${colNames(idx)}, ordered by $name.${bestSequence(idx)} to get $value for row ${args(0)}"
          case None =>
            s"I interpolated $name.${colNames(idx)}, ordered by $name.${bestSequence(idx)} row ${args(0)}"
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
    0.5
  }

}