package mimir.models

import java.io.File
import java.sql.SQLException
import java.util
import com.typesafe.scalalogging.slf4j.Logger

import mimir.algebra._
import mimir.ctables._
import mimir.util.{RandUtils,TextUtils,Timer}
import mimir.Database

import mimir.models._

import mimir.ml.spark.{SparkML, Classification, Regression}
import mimir.ml.spark.SparkML.{SparkModelGeneratorParams => ModelParams }

import scala.util._
import org.apache.spark.sql.types.StringType

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.expressions.EqualTo
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.MonotonicallyIncreasingID
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.DataFrame
import mimir.algebra.spark.OperatorTranslation
import mimir.provenance.Provenance
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.ml.PipelineModel
import mimir.exec.mode.BestGuess
import mimir.util.ExperimentalOptions
import org.apache.spark.sql.types.{IntegerType, DoubleType, FloatType, StringType, DateType, TimestampType, BooleanType, LongType, ShortType}

object SparkClassifierModel
{
  val logger = Logger(org.slf4j.LoggerFactory.getLogger(getClass.getName))
  val TRAINING_LIMIT = 10000
  val TOKEN_LIMIT = 100
  
  def availableSparkModels = Map("Classification" -> (Classification, Classification.NaiveBayesMulticlassModel _), "Regression" -> (Regression, Regression.GeneralizedLinearRegressorModel _))
  
  def train(db: Database, name: String, cols: Seq[String], query:Operator): Map[String,(Model,Int,Seq[Expression])] = 
  {
    cols.map( (col) => {
      val modelName = s"$name:$col"
      val model = 
        db.models.getOption(modelName) match {
          case Some(model) => model
          case None => {
            val (schemaWProv, modelHT) = SparkML.getDataFrameWithProvFromQuery(db, query)
            val model = new SimpleSparkClassifierModel(modelName, col, schemaWProv, modelHT)
            trainModel(db, query, model)
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

    /**
   * When the model is created, learn associations from the existing data.
   */
  def trainModel(db:Database, query:Operator, model:SimpleSparkClassifierModel)
  {
    model.sparkMLInstanceType = model.guessSparkModelType(model.guessInputType) 
    val trainingQuery = Limit(0, Some(SparkClassifierModel.TRAINING_LIMIT), query.filter(Not(IsNullExpression(Var(model.colName)))))
    val trainingDataF = db.backend.execute(trainingQuery)
    val trainingData = trainingDataF.schema.fields.filter(col => Seq(DateType, TimestampType).contains(col.dataType)).foldLeft(trainingDataF)((init, cur) => init.withColumn(cur.name,init(cur.name).cast(LongType)) )
    val (sparkMLInstance, sparkMLModelGenerator) = availableSparkModels.getOrElse(model.sparkMLInstanceType, (Classification, Classification.NaiveBayesMulticlassModel _))
    Timer.monitor(s"Train ${model.name}.${model.colName}", SparkClassifierModel.logger.info(_)){
      model.learner = Some(sparkMLModelGenerator(trainingData)(ModelParams(db, model.colName, "keep")))
      model.classifyAll(sparkMLInstance)
    }
  }
}

@SerialVersionUID(1001L)
class SimpleSparkClassifierModel(name: String, val colName:String, val schema:Seq[(String, BaseType)], val data:DataFrame)
  extends Model(name) 
  with SourcedFeedback
  with ModelCache
{
  val columns = schema.map{_._1}
  val colIdx:Int = columns.indexOf(colName)
  var classifyAllPredictions:Option[Map[String, Seq[(String, Double)]]] = None
  var learner: Option[PipelineModel] = None
  
  var sparkMLInstanceType = "Classification" 
  
  
  def getCacheKey(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue] ) : String = args(0).asString
  def getFeedbackKey(idx: Int, args: Seq[PrimitiveValue] ) : String = args(0).asString

  
 
  def guessSparkModelType(t:BaseType) : String = {
    t match {
      case TFloat() if columns.length == 1 => "Regression"
      case TInt() | TDate() | TString() | TBool() | TRowId() | TType() | TAny() | TTimestamp() | TInterval() => "Classification"
      case x => "Classification"
    }
  }
  
  def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit =
  {
    setFeedback(idx, args, v)
  }

  def isAcknowledged(idx: Int, args: Seq[PrimitiveValue]): Boolean =
    hasFeedback(idx, args)

  
  def classifyAll(sparkMLInstance:SparkML) : Unit = {
    val classifier = learner.get
    val classifyAllQuery = data.transform(sparkMLInstance.fillNullValues)
    val predictions = sparkMLInstance.applyModel(classifier, classifyAllQuery)
    //predictions.show()
    
    //evaluate acuracy
    /*val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("Test set accuracy = " + accuracy)*/
    
    val predictionsExt = sparkMLInstance.extractPredictions(classifier, predictions)
    val predictionMap = predictionsExt.groupBy(_._1).map { case (k,v) => (k,v.map(_._2))}
    classifyAllPredictions = Some(predictionMap)
    
    predictionMap.map(mapEntry => {
      setCache(0,Seq(RowIdPrimitive(mapEntry._1)), null, classToPrimitive( mapEntry._2(0)._1))
    }) 
  }

  private def classToPrimitive(value:String): PrimitiveValue = 
  {
    try {
      //mimir.parser.ExpressionParser.expr( value).asInstanceOf[PrimitiveValue]
      TextUtils.parsePrimitive(guessInputType, value)
    } catch {
      case t: Throwable => throw new Exception(s"${t.getClass.getName} while parsing primitive: $value of type: $guessInputType")
    }
  }

  def guessInputType: BaseType =
    schema(colIdx)._2

  def argTypes(idx: Int): Seq[BaseType] = List(TRowId())
  def hintTypes(idx: Int) = schema.reverse.tail.reverse.map(_._2)

  def varType(idx: Int, args: Seq[BaseType]): BaseType = guessInputType
  
  def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): PrimitiveValue =
  {
    /*println(s"-----------------------Spark Classifier Model bestGuess(idx:$idx, args:${args.mkString("(",",",")")}, hints:${hints.mkString("(",",",")")})")
    Thread.currentThread().getStackTrace.foreach(ste => println(ste.toString()))
    println("^---------------------------------- stackTrace ------------------------------------^")*/
    val rowidstr = args(0).asString
    val rowid = RowIdPrimitive(rowidstr)
    getFeedback(idx, args) match {
      case Some(v) => v
      case None => {
        getCache(idx, args, hints) match {
          case None => NullPrimitive()//throw new Exception(s"Model: $name is not trained")
          case Some(v) => v
        }
      }
    }
  }
  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): PrimitiveValue = 
  {
    val rowidstr = args(0).asString
    val rowid = RowIdPrimitive(rowidstr)
    val predictions = classifyAllPredictions match {
        case None => {
          classifyAllPredictions.get
        }
        case Some(p) => p
      }
    predictions.getOrElse(rowidstr, Seq()) match {
      case Seq() => classToPrimitive("0")
      case classes => classToPrimitive(RandUtils.pickFromWeightedList(randomness, classes))
    }   
  }
  def reason(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): String = 
  { 
    try {
      
    
    val rowidstr = args(0).asString
    val rowid = RowIdPrimitive(rowidstr)
    getFeedback(idx, args) match {
      case Some(v) =>
        s"${getReasonWho(idx,args)} told me that $name.$colName = $v on row $rowid"
      case None => 
        getCache(idx, args, hints) match {
          case None => s"The classifier isn't able to make a guess about $name.$colName, so I'm defaulting to ${classToPrimitive("0")}"
          case Some(elem) => s"I used a classifier to guess that ${name.split(":")(0)}.$colName = $elem on row $rowid"
        }
    }
    } catch {
      case t: Throwable => {
        println(t.getStackTrace.map(ste => ste.toString()).mkString("\n"))
        "error"
      }
    }
  }

  def confidence (idx: Int, args: Seq[PrimitiveValue], hints:Seq[PrimitiveValue]): Double = {
    val rowid = RowIdPrimitive(args(0).asString)
    getFeedback(idx,args) match {
      case Some(v) => 1.0
      case None => classifyAllPredictions match {
        case None => 0.0
        case Some(v) => v.get(args(0).asString) match {
          case None => 0.0
          case Some(pred) => pred.head._2/pred.maxBy(_._2)._2
        }
        
      }
    }
  }

}
