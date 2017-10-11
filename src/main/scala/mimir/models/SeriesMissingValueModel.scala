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
  val logger = Logger(org.slf4j.LoggerFactory.getLogger(getClass.getName))
  val TRAINING_LIMIT = 10000

  def train(db: Database, name: String, columns: Seq[String], query:Operator): Map[String,(Model,Int,Seq[Expression])] = 
  {
    columns.map((column) => {
      val modelName = s"$name:$column"
      var hintColumn: String = null
      val model = 
        db.models.getOption(modelName) match {
          case Some(model) => model
          case None => {
            val model = new SimpleSeriesModel(modelName, column, query)
            model.train(db)
            db.models.persist(model)
            hintColumn = model.getSeriesColumn
            logger.trace(s"$column picked $hintColumn to guess ${model.assumptionFeedback}")
            model
          }
      }
      column -> (model, 0, Seq(Var(hintColumn)))
    }).toMap
  }
  
  def isTypeCompatible(columnType: Type): Boolean = 
  {
    val columnBase = Type.rootType(columnType)
    columnBase match{
      case TInt() | TFloat() | TTimestamp() | TDate() | TInterval() => true
      case _ => false
    }
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
class SimpleSeriesModel(name: String, colName: String, query: Operator) 
  extends Model(name) 
{
  val assumptionFeedback = scala.collection.mutable.Map[String,(PrimitiveValue, PrimitiveValue, PrimitiveValue, Boolean)]()

  @transient var db: Database = null
  
  var colType: Type = null 
  var seriesColType: Type = null
  var seriesColumnName: String = null
  val colIdx:Int = query.columnNames.indexOf(colName)
  
  val rangeData = new Array[PrimitiveValue](2)

  def train(db: Database){
    this.db = db

    val querySchema = db.bestGuessSchema(query)
    colType = querySchema(colIdx)._2
    
    val findingSeries = new DetectSeries(db, 0.1)
    val seriesColumn = findingSeries.bestMatchSeriesColumn(colName, colType, query) match {
      case null => throw new ModelException("Query doesn't have a series Column, So this model is Invalid")        
      case i: String => i
    }
    seriesColumnName = seriesColumn
    
    
    seriesColType = querySchema(query.columnNames.indexOf(seriesColumn))._2

    
    val seriesColProjection = query.project(seriesColumn, colName).addColumn(("ROWID", RowIdVar().toExpression)).sort((seriesColumn, true)).filter(Not(Var(seriesColumn).isNull))
    val trainingQuery = seriesColProjection//Limit(0, Some(SeriesMissingValueModel.TRAINING_LIMIT), seriesColProjection)
    
    db.query(trainingQuery){ result => 

      var nullList: Seq[(PrimitiveValue, PrimitiveValue)] = Seq()
      var hasNullOperation = false
      var lowerBound: (PrimitiveValue, PrimitiveValue) = (NullPrimitive(), NullPrimitive())
      var upperBound: (PrimitiveValue, PrimitiveValue) = (NullPrimitive(), NullPrimitive())
      
      for(row <- result){


        if(row.tuple(1) != NullPrimitive()){
          if(rangeData(0) == null)
            rangeData(0) = row.tuple(1)
          else{
            if(Eval.applyCmp(Cmp.Lt, row.tuple(1), rangeData(0)).asBool) {rangeData(0) = row.tuple(1)}
          }
          if(rangeData(1) == null)
            rangeData(1) = row.tuple(1)
          else{
            if(Eval.applyCmp(Cmp.Gt, row.tuple(1), rangeData(1)).asBool) {rangeData(1) = row.tuple(1)}
          }
        }      
        
          
        row.tuple(1) match{
          case NullPrimitive() => { nullList = nullList :+ (row.tuple(0),row.tuple(2)) }
          case _ => { 
            if(nullList.isEmpty) {
              lowerBound = (row.tuple(0),row.tuple(1))
            }
            else{
              upperBound = (row.tuple(0), row.tuple(1))
              if(lowerBound._2 == NullPrimitive()) { lowerBound = (row.tuple(0), row.tuple(1)) }
              while(!nullList.isEmpty){
                 val nullHead = nullList.head
                 val weightedIncRatio = getPrimitiveRatio(lowerBound._1, upperBound._1, nullHead._1)
                 val guessValue = getRatioPrimitive(lowerBound._2, upperBound._2, weightedIncRatio)
                 val rowid = nullHead._2.asString
                 assumptionFeedback(rowid) = (guessValue, lowerBound._2, upperBound._2, false)
                 nullList = nullList.tail
              }
              lowerBound = (row.tuple(0),row.tuple(1))
            }
              
          }
        }
      }
      if(!nullList.isEmpty){
        nullList.foreach(x => 
          assumptionFeedback(x._2.asString) = (lowerBound._2, lowerBound._2, lowerBound._2, false)
        )
        nullList = Seq()
      }
    }
  }
  
  def argTypes(idx: Int) = Seq(TRowId())

  def varType(idx: Int, args: Seq[Type]): Type = colType 

  
  def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]  ) = {

    val rowid = RowIdPrimitive(args(0).asString)

    assumptionFeedback.get(rowid.asString) match {
      case Some(v) => v._1
      case None => { 
        colType match{
          case TDate() => NullPrimitive()// mid of date value
          case TTimestamp() => NullPrimitive()// mid of timestamp value
          case TInt() | TFloat() => db.interpreter.eval(((rangeData(1).add(rangeData(0))).div(IntPrimitive(2))))
          case _ => throw new ModelException("Column " + colName + " is "+colType+" type, does not follow a series")
        }
      }
    }
  }
  
  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]) = {

    val rowid = RowIdPrimitive(args(0).asString)
    
    val rangeValue = new Array[PrimitiveValue](2)

    assumptionFeedback.get(rowid.asString) match {
      case Some(v) => {
        rangeValue(0) = v._2; rangeValue(1) = v._3
      }
      case None => {rangeValue(0) = rangeData(0); rangeValue(1) = rangeData(1)}
    }
    colType match {
          case TDate() => NullPrimitive()// random date value(rangeValue)
          case TTimestamp() => NullPrimitive()// random timestamp value(rangeValue)
          case TInt() => {
            val inc = if((rangeValue(1).asInt-rangeValue(0).asInt)/100 < 1) 1 else ((rangeValue(1).asInt-rangeValue(0).asInt)/100).toInt
            new IntPrimitive(RandUtils.pickFromList(
                                            randomness,
                                            Seq.range(rangeValue(0).asInt, rangeValue(1).asInt+inc, inc)
                                          )) 
          }
          case TFloat() => {
            val inc = if((rangeValue(1).asDouble - rangeValue(0).asDouble)/100 == 0) 1.0 else (rangeValue(1).asDouble - rangeValue(0).asDouble)/100
            new FloatPrimitive(RandUtils.pickFromList(
                                            randomness,
                                            (rangeValue(0).asDouble to rangeValue(1).asDouble by inc).toList.toSeq)
                                          )  
          }
          case _ => throw new ModelException("Column " + colName + " is "+colType+" type, does not follow a series")
    }
  }

  def reason(idx: Int, args: Seq[PrimitiveValue],hints: Seq[PrimitiveValue]): String = {
    val rowid = RowIdPrimitive(args(0).asString)
    assumptionFeedback.get(rowid.asString) match {
      case Some(v) => {
          if(v._4) s"You told me that $name.$colName = ${v._1} on row $rowid"
        else s"I used weighted averaging on the series to guess that $name.$colName = ${v._1} on row $rowid"
      }
      case None => s"I'm not able to guess based on weighted mean $name.$colName, so defaulting using the upper and lower bound values"
    }
  }
  
  def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit = { 
    val rowid = RowIdPrimitive(args(0).asString)
    val currentValue = assumptionFeedback.getOrElse(rowid.asString, (NullPrimitive(), NullPrimitive(), NullPrimitive(), false))
    assumptionFeedback(rowid.asString) = (v, currentValue._2, currentValue._3, true)
  }

  def isAcknowledged (idx: Int, args: Seq[PrimitiveValue]): Boolean = {
    val rowid = RowIdPrimitive(args(0).asString)
    assumptionFeedback.get(rowid.asString) match {
      case Some(v) => v._4
      case None => false
    }
  }
  
  def hintTypes(idx: Int): Seq[mimir.algebra.Type] = Seq(TAny())
  
  def getPrimitiveRatio(lower: PrimitiveValue, upper: PrimitiveValue, midVal: PrimitiveValue): PrimitiveValue = {
    if(lower == upper) return FloatPrimitive(0.0)
    seriesColType match{ // can be passed as an argument!!
        case TDate() => db.interpreter.eval(IntPrimitive(TimeUtils.getDaysBetween(lower, midVal)).div(FloatPrimitive(TimeUtils.getDaysBetween(lower, upper).toDouble)))
        case TTimestamp() => db.interpreter.eval(IntPrimitive(TimeUtils.getSecondsBetween(lower, midVal)).div(FloatPrimitive(TimeUtils.getSecondsBetween(lower, upper).toDouble))) 
        case TInt() | TFloat() => FloatPrimitive(db.interpreter.evalFloat(((midVal.sub(lower)).mult(FloatPrimitive(1))).div(upper.sub(lower))))
        case _ => throw new ModelException("Column " + colName + " is "+colType+" type, is not compactible with series prediction")
      }
  }
  def getRatioPrimitive(lower: PrimitiveValue, upper: PrimitiveValue, ratio: PrimitiveValue): PrimitiveValue = {
    colType match{
        case TDate() => db.interpreter.eval(lower.add(IntervalPrimitive(Period.days((TimeUtils.getDaysBetween(lower, upper)*ratio.asDouble).toInt))))
        case TTimestamp() => db.interpreter.eval(lower.add(IntervalPrimitive(Period.seconds((TimeUtils.getSecondsBetween(lower, upper)*ratio.asDouble).toInt)))) 
        case TInt() => IntPrimitive(db.interpreter.evalFloat(lower.add((upper.sub(lower)).mult(ratio))).round)
        case TFloat() => db.interpreter.eval( lower.add((upper.sub(lower)).mult(ratio)))
        case _ => throw new ModelException("Column " + colName + " is "+colType+" type, is not compactible with series prediction")
      }
  }

  def getSeriesColumn() = seriesColumnName

}