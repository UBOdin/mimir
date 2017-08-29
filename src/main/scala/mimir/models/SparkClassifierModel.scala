package mimir.models

import java.io.File
import java.sql.SQLException
import java.util
import com.typesafe.scalalogging.slf4j.Logger

import mimir.algebra._
import mimir.ctables._
import mimir.util.{RandUtils,TextUtils,TimeUtils}
import mimir.{Analysis, Database}

import mimir.models._

import mimir.ml.spark.MultiClassClassification

import scala.util._
import org.apache.spark.sql.types.StringType

object SparkClassifierModel
{
  val logger = Logger(org.slf4j.LoggerFactory.getLogger(getClass.getName))
  val TRAINING_LIMIT = 10000
  val TOKEN_LIMIT = 100

  def train(db: Database, name: String, cols: Seq[String], query:Operator): Map[String,(Model,Int,Seq[Expression])] = 
  {
    cols.map( (col) => {
      val modelName = s"$name:$col"
      val model = 
        db.models.getOption(modelName) match {
          case Some(model) => model
          case None => {
            val model = new SimpleSparkClassifierModel(modelName, col, query)
            model.train(db)
            db.models.persist(model)
            model
          }
        }

      col -> (
        model,                         // The model for the column
        0,                             // The model index of the column's replacement variable
        query.columnNames.map(Var(_))  // 'Hints' for the model -- All of the remaining column values
      )
    }).toMap
  }

  

  
}

@SerialVersionUID(1000L)
class SimpleSparkClassifierModel(name: String, colName: String, query: Operator)
  extends Model(name) 
  with NeedsReconnectToDatabase 
{
  val feedback = scala.collection.mutable.Map[String,PrimitiveValue]()
  val classificationCache = scala.collection.mutable.Map[String,PrimitiveValue]()
  val colIdx:Int = query.columnNames.indexOf(colName)
  val classifyUpFrontAndCache = true
  var learner: Option[MultiClassClassification.ClassifierModel] = None
  
  @transient var db: Database = null

  
  /**
   * When the model is created, learn associations from the existing data.
   */
  def train(db:Database)
  {
    this.db = db
    TimeUtils.monitor(s"Train $name.$colName", WekaModel.logger.info(_)){
      val trainingQuery = Limit(0, Some(SparkClassifierModel.TRAINING_LIMIT), Project(Seq(ProjectArg(colName, Var(colName))), query))
      learner = Some(MultiClassClassification.NaiveBayesMulticlassModel(MultiClassClassification.prepareValueStr _, MultiClassClassification.getSparkTypeStr _)(trainingQuery, db, colName))
    }
  }

  
  def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit =
  {
    val rowid = args(0).asString
    feedback(rowid) = v
  }

  def isAcknowledged(idx: Int, args: Seq[PrimitiveValue]): Boolean =
    feedback contains(args(0).asString)

  
  private def classify(rowid: RowIdPrimitive, rowValueHints: Seq[PrimitiveValue]): Seq[(String,Double)] = {
     {if(rowValueHints.isEmpty){
       MultiClassClassification.extractPredictions(learner.get, MultiClassClassification.classifydb(learner.get, 
                Project(Seq(ProjectArg(colName, Var(colName))),
                  Select(
                    Comparison(Cmp.Eq, RowIdVar(), rowid),
                    query)
                ), db, MultiClassClassification.prepareValueStr _, MultiClassClassification.getSparkTypeStr _)) 
     } else { 
       MultiClassClassification.extractPredictions(learner.get,
           MultiClassClassification.classify(learner.get,  db.bestGuessSchema(query).filter(_._1.equals(colName)), List(rowid +: rowValueHints), MultiClassClassification.prepareValueStr _, MultiClassClassification.getSparkTypeStr _))
     }} match {
         case Seq() => Seq()
         case x => x.head
       }
  }
  
  def classifyAll() : Unit = {
    val classifier = learner.get
    val classifyAllQuery = Project(Seq(ProjectArg(colName, Var(colName))), query)
    val tuples = db.query(classifyAllQuery, mimir.exec.mode.BestGuess)(results => {
      results.toList.map(row => (row.provenance :: row.tuple.toList).toSeq)
    })
    val predictions = MultiClassClassification.classify(classifier, db.bestGuessSchema(query).filter(_._1.equals(colName)), tuples, MultiClassClassification.prepareValueStr _, MultiClassClassification.getSparkTypeStr _)
    val sqlContext = MultiClassClassification.getSparkSqlContext()
    import sqlContext.implicits._  
    
    //method 1: window, rank, and drop
    /*import org.apache.spark.sql.functions.row_number
    import org.apache.spark.sql.expressions.Window
    val w = Window.partitionBy($"rowid").orderBy($"probability".desc)
    val topPredictionsForEachRow = predictions.withColumn("rn", row_number.over(w)).where($"rn" === 1).drop("rn")88
    */
    
    //method 2: group and first
    import org.apache.spark.sql.functions.first
    val topPredictionsForEachRow = predictions.sort($"rowid", $"probability".desc).groupBy($"rowid").agg(first(predictions.columns.tail.head).alias(predictions.columns.tail.head), predictions.columns.tail.tail.map(col => first(col).alias(col) ):_*) 
   
    topPredictionsForEachRow.select("rowid", "predictedLabel").collect().map(row => {
      classificationCache(row.getString(0)) = mimir.parser.ExpressionParser.expr( row.getString(1) ).asInstanceOf[PrimitiveValue]
    })
    db.models.persist(this)   
  }

  private def classToPrimitive(value:String): PrimitiveValue = 
  {
    //mimir.parser.ExpressionParser.expr( value).asInstanceOf[PrimitiveValue]
    TextUtils.parsePrimitive(guessInputType, value)
  }

  def guessInputType: Type =
    db.bestGuessSchema(query)(colIdx)._2

  def argTypes(idx: Int): Seq[Type] = List(TRowId())
  def hintTypes(idx: Int) = db.typechecker.schemaOf(query).map(_._2)

  def varType(idx: Int, args: Seq[Type]): Type = guessInputType
  
  def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): PrimitiveValue =
  {
    val rowidstr = args(0).asString
    val rowid = RowIdPrimitive(rowidstr)
    feedback.get(rowid.asString) match {
      case Some(v) => v
      case None => {
        classificationCache.get(rowidstr) match {
          case None => {
            if(classifyUpFrontAndCache){
                  classifyAll()
                  classificationCache.get(rowidstr) match {
                    case None => {
                      val cacheNone = classToPrimitive("0")
                      classificationCache(rowidstr) = cacheNone
                      cacheNone
                    }
                    case Some(v) => v
                  }
            }
            else{
              val classes = classify(rowid, hints)
              val res =  if (classes.isEmpty) { "0" }
                else {
                  classify(rowid, hints).head._1
                }
              classToPrimitive(res)
            }
          }
          case Some(v) => v
        }
      }
    }
  }
  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): PrimitiveValue = 
  {
    val rowid = RowIdPrimitive(args(0).asString)
    val classes = classify(rowid, hints)
    val res = if (classes.isEmpty) { "0" }
              else {
                RandUtils.pickFromWeightedList(
                  randomness,
                  classes
                )
              }
    classToPrimitive(res)
  }
  def reason(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): String = 
  {
    val rowid = RowIdPrimitive(args(0).asString)
    feedback.get(rowid.asString) match {
      case Some(v) =>
        s"You told me that $name.$colName = $v on row $rowid"
      case None => 
        val classes = classify(rowid.asInstanceOf[RowIdPrimitive], hints)
        val total:Double = classes.map(_._2).fold(0.0)(_+_)
        if (classes.isEmpty) { 
          val elem = classToPrimitive("0")
          s"The classifier isn't able to make a guess about $name.$colName, so I'm defaulting to $elem"
        } else {
          val choice = classes.maxBy(_._2)
          val elem = classToPrimitive(choice._1)
          s"I used a classifier to guess that ${name.split(":")(0)}.$colName = $elem on row $rowid (${choice._2} out of ${total} votes)"
        }
    }
  }

  def reconnectToDatabase(db: Database): Unit = {
    this.db = db
  }

  
}
