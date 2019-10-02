package mimir.models;

import java.io.File
import java.sql.SQLException
import java.util

import scala.collection.JavaConversions._
import scala.util._
import scala.util.Random

import com.typesafe.scalalogging.Logger
import org.joda.time.{DateTime, Seconds, Days, Period}
import com.typesafe.scalalogging.Logger

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
import mimir.exec.spark.RAToSpark
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row

//Upgrade: Move the series column detection to SeriesMissingValueModel

object SeriesMissingValueModel
{
  val logger = Logger(org.slf4j.LoggerFactory.getLogger("mimir.models.SeriesMissingValueModel"))

  def train(db: Database, name: ID, columns: Seq[ID], query:Operator, humanReadableName:String): Map[ID,(Model,Int,Seq[Expression])] = 
  {
    logger.debug(s"Train on: $query")
    val (schemaWProv, modelHT) = SparkUtils.getDataFrameWithProvFromQuery(db, query)
    val model = new SimpleSeriesModel(name, columns, schemaWProv, modelHT, humanReadableName)
    val usefulColumns = trainModel( modelHT, columns, schemaWProv, model)
    columns.zip(usefulColumns)
      .zipWithIndex
      .filter(_._1._2)
      .map { case ((column, _), idx) => 
        (column -> (model, idx, Seq()))
      }
      .toMap
  }
  def trainModel(queryDf: DataFrame, columns:Seq[ID], schema:Seq[(ID, Type)], model:SimpleSeriesModel) =
  {
    val predictions = 
      columns.zipWithIndex.map { case (col, idx) => 
        DetectSeries.bestMatchSeriesColumn(
          col, 
          schema.toMap.get(col).get, 
          queryDf,
          schema,
          0.1
        )
      }
    model.train(predictions)
  }
}

case class SeriesColumnItem(columnName: ID, reason: String, score: Double)

/** 
 * A model performs estimation of missing value column based on the column that follows a series.
 * Best Guess : Performs the best guess based on the weighted-average of upper and lower bound values.
 * Sample : Picks a random value within the range of uppper and lower bound
 * Train : Performs best guess on missing value fields. For each missing value, a map in created
 * 				 [ ROWID -> (Best Guess Value, Lower Bound Value, Upper Bound Value, Recieved Feedback) ] 
 *
 *  */

@SerialVersionUID(1000L)
class SimpleSeriesModel(name: ID, val seriesCols:Seq[ID], val querySchema: Seq[(ID, Type)], queryDf: DataFrame, humanReadableName:String) 
  extends Model(name) 
  with SourcedFeedback
  with ModelCache
{
  
  var predictions:Seq[Dataset[(ID, Double)]] = Seq()
  
  
  val colNames = querySchema.map { x => x._1 }

  def getCacheKey(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue] ) : ID = 
    ID(s"${idx}_${args(0).asString}_${hints(0).asString}")
  def getFeedbackKey(idx: Int, args: Seq[PrimitiveValue] ) : ID = 
    ID(s"${idx}_${args(0).asString}")

  def train(predictionsDs:Seq[Dataset[(ID, Double)]] ): Seq[Boolean] = 
  {
    predictions = predictionsDs
    SeriesMissingValueModel.logger.debug(s"Trained: $predictions")
    predictions.map(!_.limit(1).collect().isEmpty)
  }

  def validFor: Set[ID] =
  {
    seriesCols
      .zip(predictions)
      .flatMap { 
        case (col, Seq()) => None
        case (col, _) => Some(col)
      }
      .toSet
  }
 
  
  def interpolate(idx: Int, args:Seq[PrimitiveValue], series: ID): PrimitiveValue =
    interpolate(idx, args(0).asInstanceOf[RowIdPrimitive], series)
  def interpolate(idx: Int, rowid:RowIdPrimitive, series: ID): PrimitiveValue =
  {
    val colName = seriesCols(idx)
    val sp2m : (String, Row) => PrimitiveValue = (colName, row) => {
      row match {
        case null => NullPrimitive()
        case _ => SparkUtils.convertField(
          querySchema.toMap.get(ID(colName)).get match {
            case TDate() => TInt()
            case TTimestamp() => TInt()
            case x => x
          },
          row, 
          row.fieldIndex(colName)) /*match {
            case np@NullPrimitive() => np
            case x => querySchema.toMap.get(colName).get match {
              case TDate() => SparkUtils.convertDate(x.asLong)
              case TTimestamp() => SparkUtils.convertTimestamp(x.asLong)
              case _ => x
            } 
          }*/
      }
    }
    val m2sp : PrimitiveValue => Any = prim => RAToSpark.mimirPrimitiveToSparkExternalRowValue(prim)
    val sprowid = m2sp(rowid)
    val rowIdVar = col(colNames.last.id)//(monotonically_increasing_id()+1).alias(RowIdVar().toString()).cast(OperatorTranslation.getSparkType(rowIdType))
    val rowDF = queryDf.filter(rowIdVar === sprowid )
    val key = sp2m(series.id,rowDF.select(series.id).limit(1).collect().headOption.getOrElse(null))
    val nkey = querySchema.toMap.get(series).get match {
      case TDate() => SparkUtils.convertDate(key.asLong)
      case TTimestamp() => SparkUtils.convertTimestamp(key.asLong)
      case _ => key
    }
    SeriesMissingValueModel.logger.debug(
     s"Interpolate $rowid with key: $nkey for column: (${colName} -> ${querySchema.toMap.get(colName).get}) with series: ($series -> ${querySchema.toMap.get(series.id)})")
    if(key == NullPrimitive()){ return NullPrimitive(); }
    val low = 
      queryDf.filter((col(series.id) <= lit(m2sp(key))).and(not(isnull(col(colName.id)))).and(rowIdVar =!= sprowid))
          .sort(desc(series.id))
          .limit(1)
          .select(series.id, colName.id).collect().map( row => {
            (querySchema.toMap.get(series).get match {
              case TDate() => SparkUtils.convertDate(sp2m(series.id,row).asLong)
              case TTimestamp() => SparkUtils.convertTimestamp(sp2m(series.id,row).asLong)
              case _ => sp2m(series.id,row)
            },
            querySchema.toMap.get(colName).get match {
              case TDate() => SparkUtils.convertDate(sp2m(colName.id,row).asLong)
              case TTimestamp() => SparkUtils.convertTimestamp(sp2m(colName.id,row).asLong)
              case _ => sp2m(colName.id,row)
            } )
          } ).toSeq
    val high = 
      queryDf.filter((col(series.id) >= lit(m2sp(key))).and(not(isnull(col(colName.id)))).and(rowIdVar =!= sprowid))
          .sort(asc(series.id))
          .limit(1)
          .select(series.id, colName.id).collect().map( row => {
            (querySchema.toMap.get(series).get match {
              case TDate() => SparkUtils.convertDate(sp2m(series.id,row).asLong)
              case TTimestamp() => SparkUtils.convertTimestamp(sp2m(series.id,row).asLong)
              case _ => sp2m(series.id,row)
            },
            querySchema.toMap.get(colName).get match {
              case TDate() => SparkUtils.convertDate(sp2m(colName.id,row).asLong)
              case TTimestamp() => SparkUtils.convertTimestamp(sp2m(colName.id,row).asLong)
              case _ => sp2m(colName.id,row)
            } )
          } ).toSeq

    
    SeriesMissingValueModel.logger.debug(s"   -> low = $low; high = $high")
   
    val result = (low, high) match {
      case (Seq(), Seq()) => NullPrimitive()
      case (Seq((_, low_v)), Seq())  => low_v
      case (Seq(), Seq((_, high_v))) => high_v
      case (Seq((low_k, low_v)), Seq((high_k, high_v))) => {
        val ratio = DetectSeries.ratio(low_k, nkey, high_k)
        SeriesMissingValueModel.logger.debug(s"   -> ratio = $ratio")
        DetectSeries.interpolate(low_v, ratio, high_v)
      }
    }
    SeriesMissingValueModel.logger.debug(s"result = $result; rowid = $rowid")
    setCache(idx, Seq(rowid), Seq(StringPrimitive(series.id)),result)
    result
  }

  def bestSequence(idx: Int): ID = {
    val df = predictions(idx)
    val bestSeq = df.sort(asc(df.columns(1))).limit(1).head._1
    SeriesMissingValueModel.logger.debug(s"bestSeq for col:${seriesCols(idx)} => $bestSeq") 
	  bestSeq
  }

  def argTypes(idx: Int) = Seq(TRowId())

  def varType(idx: Int, args: Seq[Type]): Type = querySchema.toMap.get(seriesCols(idx)).get
  
  def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]  ): PrimitiveValue = 
  {
    getFeedback(idx, args) match {
      case Some(v) => v
      case None => {
        val bestSeries = bestSequence(idx)
        getCache(idx, args, Seq(StringPrimitive(bestSeries.id))) match {
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
        getCache(idx, args, Seq(StringPrimitive(series.id))) match {
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
        s"You told me that $name.${seriesCols(idx)} = ${value} on row ${args(0)} (${args(0).getType})"
      case None => {
        val bestSeries = bestSequence(idx)
        getCache(idx, args, Seq(StringPrimitive(bestSeries.id))) match {
          case Some(value) => 
            s"I interpolated $humanReadableName.${seriesCols(idx)}, ordered by $humanReadableName.${bestSequence(idx)} to get $value for row ${args(0)}"
          case None =>{
            //s"I interpolated $name.${colNames(idx)}, ordered by $name.${bestSequence(idx)} row ${args(0)}"
            s"I interpolated $humanReadableName.${seriesCols(idx)}, ordered by $humanReadableName.${bestSeries} to get ${interpolate(idx, args, bestSeries)} for row ${args(0)}"
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