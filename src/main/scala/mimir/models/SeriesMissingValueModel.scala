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
{
  @transient var db: Database = null

  val cache = 
    colNames.map { _ => scala.collection.mutable.Map[RowIdPrimitive, (PrimitiveValue, Boolean)]() }
  var predictions = 
    colNames.map { _ => Seq[(String,Double)]() }
  var querySchema:Seq[Type] = 
    colNames.map { _ => TAny() }

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
          .filter(Var(series).lt(key).and(Var(colNames(idx)).isNull.not))
          .sort(series -> false)
          .limit(1)
          .project(series, colNames(idx))
      ) { results => if(results.hasNext){ val t = results.next.tuple; Some((t(0), t(1))) } else { None } }
    val high = 
      db.query(
        query
          .filter(Var(series).gt(key).and(Var(colNames(idx)).isNull.not))
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

    val rowid = args(0).asInstanceOf[RowIdPrimitive]
    cache(idx).get(rowid) match {
      case Some((value, isFeedback)) => value
      case None => { 
        val ret = interpolate(idx, rowid, bestSequence(idx))
        cache(idx).put(rowid, (ret, false))
        ret
      }
    }
  }
  
  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): PrimitiveValue = 
  {
    val rowid = args(0).asInstanceOf[RowIdPrimitive]
    cache(idx).get(rowid) match {
      case Some((ret, true)) => return ret
      case _ => {
        // this should probably be scaled by variance.  For now... just pick entirely at random
        val series = RandUtils.pickFromList(randomness, predictions(idx).map { _._1 })
        return interpolate(idx, rowid, series)
      }
    }
  }

  def reason(idx: Int, args: Seq[PrimitiveValue],hints: Seq[PrimitiveValue]): String = {
    val rowid = args(0).asInstanceOf[RowIdPrimitive]

    SeriesMissingValueModel.logger.debug(s"Best guess for $rowid -> Cache = ${cache(idx).get(rowid)}")

    cache(idx).get(rowid) match {
      case Some((value, true)) =>
        s"You told me that $name.${colNames(idx)} = ${value} on row $rowid"
      case Some((value, false)) =>
        s"I interpolated $name.${colNames(idx)}, ordered by $name.${bestSequence(idx)} to get $value for row $rowid"
      case None => 
        val value = bestGuess(idx, args, hints)
        s"I interpolated $name.${colNames(idx)}, ordered by $name.${bestSequence(idx)} to get $value for row $rowid"
    }
  }
  
  def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit = { 
    cache(idx).put(args(0).asInstanceOf[RowIdPrimitive], (v, true))
  }

  def isAcknowledged (idx: Int, args: Seq[PrimitiveValue]): Boolean = {
    cache(idx).get(args(0).asInstanceOf[RowIdPrimitive]) match {
      case Some((_, true)) => true
      case _ => false
    }
  }
  
  def hintTypes(idx: Int): Seq[mimir.algebra.Type] = Seq()
  

}