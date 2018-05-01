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
            val modelHT = db.query(query)(results => {
              val schema = results.schema :+ (Provenance.rowidColnameBase,TRowId())
              val reslist = results.toList
              (schema, reslist.map( row => row.tuple :+ row.provenance).toSeq )
            })
            val model = new SimpleSparkClassifierModel(modelName, col, modelHT._1, modelHT._2)
            trainModel(db, query, model)
            //db.models.persist(model)
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
    val sparkMLInstance = availableSparkModels.getOrElse(model.sparkMLInstanceType, (Classification, Classification.NaiveBayesMulticlassModel()))._1
    def sparkMLModelGenerator = availableSparkModels.getOrElse(model.sparkMLInstanceType, (Classification, Classification.NaiveBayesMulticlassModel()))._2
    Timer.monitor(s"Train ${model.name}.${model.colName}", SparkClassifierModel.logger.info(_)){
      val trainingQuery = Limit(0, Some(SparkClassifierModel.TRAINING_LIMIT), Sort(Seq(SortColumn(Function("random", Seq()), true)),  query.filter(Not(IsNullExpression(Var(model.colName))))))
      model.learner = Some(sparkMLModelGenerator(ModelParams(trainingQuery, db, model.colName, "keep")))
      model.classifyAll(sparkMLInstance)
    }
  }



}

@SerialVersionUID(1001L)
class SimpleSparkClassifierModel(name: String, val colName:String, val schema:Seq[(String, Type)], val trainingData:Seq[Seq[PrimitiveValue]])
  extends Model(name) 
  with SourcedFeedback
  with ModelCache
{
  val columns = schema.map{_._1}
  val colIdx:Int = columns.indexOf(colName)
  val classifyUpFrontAndCache = true
  var classifyAllPredictions:Option[Map[String, Seq[(String, Double)]]] = None
  var learner: Option[PipelineModel] = None
  
  var sparkMLInstanceType = "Classification" 
  
  
  def getCacheKey(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue] ) : String = args(0).asString
  def getFeedbackKey(idx: Int, args: Seq[PrimitiveValue] ) : String = args(0).asString

  
 
  def guessSparkModelType(t:Type) : String = {
    t match {
      case TFloat() if columns.length == 1 => "Regression"
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
     /*{if(rowValueHints.isEmpty){
       sparkMLInstance.extractPredictions(learner.get, sparkMLInstance.applyModel(learner.get, 
                org.apache.spark.sql.catalyst.plans.logical.Filter(
                    EqualTo(org.apache.spark.sql.catalyst.expressions.Cast(Alias(MonotonicallyIncreasingID(),"ROWID")(),StringType,None), Literal(rowid.toString())),
                    plan)
                )) 
     } else { 
       sparkMLInstance.extractPredictions(learner.get,
           sparkMLInstance.applyModel(learner.get,  OperatorTranslation.mimirOpToSparkOp(HardTable(schema, Seq(rowValueHints)))))
     }} match {
         case Seq() => Seq()
         case x => x.unzip._2
       }*/Seq()
  }
  
  
  
  
  def classifyAll(sparkMLInstance:SparkML) : Unit = {
    val classifier = learner.get
    val classifyAllQuery = sparkMLInstance.getSparkSqlContext().createDataFrame(
      sparkMLInstance.getSparkSession().parallelize(trainingData.map( row => {
        Row(row.zip(schema).map(value => sparkMLInstance.getNative(value._1, value._2._2) ):_*)
      })), StructType(schema.map(col => StructField(col._1, OperatorTranslation.getSparkType(col._2), true))))//Project(Seq(ProjectArg(colName, Var(colName))), query)
    val predictions = sparkMLInstance.applyModel(classifier, classifyAllQuery)
    //predictions.show()
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
    schema(colIdx)._2

  def argTypes(idx: Int): Seq[Type] = List(TRowId())
  def hintTypes(idx: Int) = schema.reverse.tail.reverse.map(_._2)

  def varType(idx: Int, args: Seq[Type]): Type = guessInputType
  
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
          case None => {
            if(classifyUpFrontAndCache && classifyAllPredictions.isEmpty ){
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
