package mimir.models

import java.io.File
import java.sql.SQLException
import java.util
import com.typesafe.scalalogging.slf4j.Logger

import mimir.algebra._
import mimir.ctables._
import mimir.util.{RandUtils,TextUtils,Timer}
import mimir.{Analysis, Database}

import mimir.models._

import mimir.ml.spark.{SparkML, Classification, Regression}
import mimir.ml.spark.SparkML.{SparkModelGeneratorParams => ModelParams }

import scala.util._
import org.apache.spark.sql.types.StringType

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

object SparkClassifierModel
{
  val logger = Logger(org.slf4j.LoggerFactory.getLogger(getClass.getName))
  val TRAINING_LIMIT = 10000
  val TOKEN_LIMIT = 100
  val availableSparkModels = Map("Classification" -> (Classification, Classification.NaiveBayesMulticlassModel()), "Regression" -> (Regression, Regression.GeneralizedLinearRegressorModel()))
  
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

@SerialVersionUID(1001L)
class SimpleSparkClassifierModel(name: String, colName: String, query: Operator)
  extends Model(name) 
  with NeedsReconnectToDatabase 
  with SourcedFeedback
  with ModelCache
{
  val colIdx:Int = query.columnNames.indexOf(colName)
  val classifyUpFrontAndCache = true
  var classifyAllPredictions:Option[Map[String, Seq[(String, Double)]]] = None
  var learner: Option[SparkML.SparkModel] = None
  
  var sparkMLInstanceType = "Classification" 
  
  @transient var db: Database = null
  
  def sparkMLInstance = SparkClassifierModel.availableSparkModels.getOrElse(sparkMLInstanceType, (Classification, Classification.NaiveBayesMulticlassModel()))._1
  def sparkMLModelGenerator = SparkClassifierModel.availableSparkModels.getOrElse(sparkMLInstanceType, (Classification, Classification.NaiveBayesMulticlassModel()))._2
  
  def getCacheKey(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue] ) : String = args(0).asString
  def getFeedbackKey(idx: Int, args: Seq[PrimitiveValue] ) : String = args(0).asString

  /**
   * When the model is created, learn associations from the existing data.
   */
  def train(db:Database)
  {
    this.db = db
    sparkMLInstanceType = guessSparkModelType(guessInputType) 
    Timer.monitor(s"Train $name.$colName", SparkClassifierModel.logger.info(_)){
      val trainingQuery = Limit(0, Some(SparkClassifierModel.TRAINING_LIMIT), Sort(Seq(SortColumn(Function("random", Seq()), true)),  query.filter(Not(IsNullExpression(Var(colName))))))
      learner = Some(sparkMLModelGenerator(ModelParams(trainingQuery, db, colName, "keep")))
    }
  }
 
  def guessSparkModelType(t:Type) : String = {
    t match {
      case TFloat() if query.columnNames.length == 1 => "Regression"
      case TInt() | TDate() | TString() | TBool() | TRowId() | TType() | TAny() | TTimestamp() | TInterval() => "Classification"
      case TUser(name) => guessSparkModelType(mimir.algebra.TypeRegistry.registeredTypes(name)._2)
      case x => "Classification"
    }
  }
  
  def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit =
  {
    setFeedback(idx, args, v)
  }

  def isAcknowledged(idx: Int, args: Seq[PrimitiveValue]): Boolean =
    hasFeedback(idx, args)

  
  private def classify(rowid: RowIdPrimitive, rowValueHints: Seq[PrimitiveValue]): Seq[(String,Double)] = {
     {if(rowValueHints.isEmpty){
       sparkMLInstance.extractPredictions(learner.get, sparkMLInstance.applyModelDB(learner.get, 
                Select(
                    Comparison(Cmp.Eq, RowIdVar(), rowid),
                    query)
                , db)) 
     } else { 
       sparkMLInstance.extractPredictions(learner.get,
           sparkMLInstance.applyModel(learner.get,  ("rowid", TString()) +: db.typechecker.schemaOf(query).filterNot(_._1.equalsIgnoreCase("rowid")), List(List(rowid, rowValueHints(colIdx)))))
     }} match {
         case Seq() => Seq()
         case x => x.unzip._2
       }
  }
  
  def classifyAll() : Unit = {
    val classifier = learner.get
    val classifyAllQuery = query//Project(Seq(ProjectArg(colName, Var(colName))), query)
    val predictions = sparkMLInstance.applyModelDB(classifier, classifyAllQuery, db)
   
    //val sqlContext = MultiClassClassification.getSparkSqlContext()
    //import sqlContext.implicits._  
    
    //method 1: window, rank, and drop
    /*import org.apache.spark.sql.functions.row_number
    import org.apache.spark.sql.expressions.Window
    val w = Window.partitionBy($"rowid").orderBy($"probability".desc)
    val topPredictionsForEachRow = predictions.withColumn("rn", row_number.over(w)).where($"rn" === 1).drop("rn")88
    */
    
    //method 2: group and first
    /*import org.apache.spark.sql.functions.first
    val topPredictionsForEachRow = predictions.sort($"rowid", $"probability".desc).groupBy($"rowid").agg(first(predictions.columns.tail.head).alias(predictions.columns.tail.head), predictions.columns.tail.tail.map(col => first(col).alias(col) ):_*) 
   
    topPredictionsForEachRow.select("rowid", "predictedLabel").collect().map(row => {
      classificationCache(row.getString(0)) = classToPrimitive( row.getString(1) )
    })*/
       
    //method 3: manually
    val predictionsExt = sparkMLInstance.extractPredictions(classifier, predictions)
    val predictionMap = predictionsExt.groupBy(_._1).map { case (k,v) => (k,v.map(_._2))}
    classifyAllPredictions = Some(predictionMap)
    
    predictionMap.map(mapEntry => {
      setCache(0,Seq(RowIdPrimitive(mapEntry._1)), null, classToPrimitive( mapEntry._2(0)._1))
    })
    
    db.models.persist(this)   
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

  def guessInputType: Type =
    db.typechecker.schemaOf(query)(colIdx)._2

  def argTypes(idx: Int): Seq[Type] = List(TRowId())
  def hintTypes(idx: Int) = db.typechecker.schemaOf(query).map(_._2)

  def varType(idx: Int, args: Seq[Type]): Type = guessInputType
  
  def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): PrimitiveValue =
  {
    val rowidstr = args(0).asString
    val rowid = RowIdPrimitive(rowidstr)
    getFeedback(idx, args) match {
      case Some(v) => v
      case None => {
        getCache(idx, args, hints) match {
          case None => {
            if(classifyUpFrontAndCache && classifyAllPredictions.isEmpty ){
              classifyAll()
              getCache(idx, args, hints).getOrElse(classify(rowid, hints) match {
                case Seq() => classToPrimitive("0")
                case x => classToPrimitive(x.head._1)
              })
            }
            else if(classifyUpFrontAndCache)
              classify(rowid, hints) match {
                case Seq() => classToPrimitive("0")
                case x => classToPrimitive(x.head._1)
              }
            else{
              val classes = classify(rowid, hints)
              val res =  if (classes.isEmpty) { "0" }
              else {
                classes.head._1
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
    val rowidstr = args(0).asString
    val rowid = RowIdPrimitive(rowidstr)
    (if(classifyUpFrontAndCache){
      val predictions = classifyAllPredictions match {
        case None => {
          classifyAll()
          classifyAllPredictions.get
        }
        case Some(p) => p
      }
      predictions.getOrElse(rowidstr, Seq())
    }
    else{
      classify(rowid, hints)
    }) match {
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
        val selem = getCache(idx, args, hints) match {
          case None => {
            if(classifyUpFrontAndCache && cache.isEmpty ){
              classifyAll()
              getCache(idx, args, hints)
            }
            else if(classifyUpFrontAndCache)
              None
            else{
              val classes = classify(rowid, hints)
              if (classes.isEmpty) {
                None
              }
              else {
                Some(classToPrimitive(classes.head._1))
              }  
            }
          }
          case somev@Some(v) => somev
        }      
        selem match {
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

  def reconnectToDatabase(db: Database): Unit = {
    this.db = db
  }

  def confidence (idx: Int, args: Seq[PrimitiveValue], hints:Seq[PrimitiveValue]): Double = {
    val rowid = RowIdPrimitive(args(0).asString)
    getFeedback(idx,args) match {
      case Some(v) => 1.0
      case None => {
        val classes = classify(rowid, hints)
        if (classes.isEmpty) {
          0.0
        }
        else {
          classes.head._2/classes.maxBy(_._2)._2
        }
      }
    }
  }

}
