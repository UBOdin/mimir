package mimir.models;

import scala.util.Random

import mimir.algebra._
import mimir.util._
import mimir.Database
import mimir.ml.spark.SparkML
import mimir.util.TextUtils.Levenshtein
import mimir.ml.spark.SparkML.{SparkModelGeneratorParams => ModelParams }

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import mimir.algebra.spark.OperatorTranslation
import mimir.provenance.Provenance

/**
 * A model representing a key-repair choice.
 * 
 * The index is ignored.
 * The one argument is a value for the key.  
 * The return value is an integer identifying the ordinal position of the selected value, starting with 0.
 */
@SerialVersionUID(1001L)
class PickerModel(override val name: String, resultColumn:String, pickFromCols:Seq[String], colTypes:Seq[Type], useClassifier:Option[(() => SparkML, SparkML.SparkModelGenerator)], classifyUpFrontAndCache:Boolean, source: Operator) 
  extends Model(name) 
  with Serializable
  with FiniteDiscreteDomain
  with SourcedFeedback
  with ModelCache
{
  
  val TRAINING_LIMIT = 10000
  var classifierModel: Option[SparkML.SparkModel] = None
  var sparkMLInstance: Option[() => SparkML] = None
  var classifyAllPredictions:Option[Map[String, Seq[(String, Double)]]] = None 
  var schema:Seq[(String, Type)] = null  
  var trainingData:Seq[Seq[PrimitiveValue]] = Seq()
  
  def getCacheKey(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue] ) : String = args(0).asString
  def getFeedbackKey(idx: Int, args: Seq[PrimitiveValue] ) : String = args(0).asString
 
  private def classToPrimitive(value:String): PrimitiveValue = {
    try {
      TextUtils.parsePrimitive(colTypes(0), value)
    } catch {
      case t: Throwable => throw new Exception(s"${t.getClass.getName} while parsing primitive: $value of type: ${colTypes(0)}")
    }
  }
  
  private def pickFromArgs(args: Seq[PrimitiveValue], value:Option[PrimitiveValue] = None) : PrimitiveValue = {
    value match {
      case None => 
        pickFromCols.zipWithIndex.foldLeft(NullPrimitive():PrimitiveValue)( (init, elem) => init match {
          case NullPrimitive() => args(elem._2+1)
          case x => x
        })
      case Some(v) => 
        pickFromCols.zipWithIndex.foldLeft(NullPrimitive():PrimitiveValue)( (init, elem) => (init, args(elem._2+1), v) match {
          case (NullPrimitive(), NullPrimitive(), _) => init
          case (NullPrimitive(), _, _) => args(elem._2+1)
          case (_, NullPrimitive(), _) => init
          case (e1, e2, _) if e1.equals(e2) => {
            val fbh = FeedbackSource.feedbackSource
            FeedbackSource.feedbackSource = FeedbackSource.groundSource
            if(!hasFeedback(0, args))
              setFeedback(0, args, e1)
            FeedbackSource.feedbackSource = fbh
            e1
          }
          case (p1@IntPrimitive(l1), p2@IntPrimitive(l2), IntPrimitive(l3)) => if(Math.abs(l1-l3)<Math.abs(l2-l3)) p1 else p2
          case (p1@FloatPrimitive(f1), p2@FloatPrimitive(f2), FloatPrimitive(f3)) => if(Math.abs(f1-f3)<Math.abs(f2-f3)) p1 else p2
          case (p1@StringPrimitive(s1), p2@StringPrimitive(s2), StringPrimitive(s3)) => if(Levenshtein.distance(s1, s3)<Levenshtein.distance(s2, s3)) p1 else p2
          case (d1:DatePrimitive, d2:DatePrimitive, d3:DatePrimitive) => if(Math.abs(d1.asDateTime.getMillis-d3.asDateTime.getMillis)<Math.abs(d2.asDateTime.getMillis-d3.asDateTime.getMillis)) d1 else d2
          case (d1:TimestampPrimitive, d2:TimestampPrimitive, d3:TimestampPrimitive) => if(Math.abs(d1.asDateTime.getMillis-d3.asDateTime.getMillis)<Math.abs(d2.asDateTime.getMillis-d3.asDateTime.getMillis)) d1 else d2
          case (d1:IntervalPrimitive, d2:IntervalPrimitive, d3:IntervalPrimitive) => if(Math.abs(d1.asInterval.getMillis-d3.asInterval.getMillis)<Math.abs(d2.asInterval.getMillis-d3.asInterval.getMillis)) d1 else d2
          case (p1@BoolPrimitive(b1), p2@BoolPrimitive(b2), BoolPrimitive(b3)) => if(b1 && b3)p1 else p2 
          case (p1@RowIdPrimitive(s1), p2@RowIdPrimitive(s2), RowIdPrimitive(s3)) => if(s1.equals(s3)) p1 else p2
          case (p1@TypePrimitive(t1), p2@TypePrimitive(t2), TypePrimitive(t3)) => if(t1.equals(t3)) p1 else p2 
          case _ => throw new RAException("Something really wrong is going on")
        }) match {
          case NullPrimitive() => v
          case x => x
        }
    }
  }

  def argTypes(idx: Int) = {
      Seq(TRowId()).union(colTypes)
  }
  def varType(idx: Int, args: Seq[Type]) = colTypes(idx)
  def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]  ) = {
    val rowid = args(0).asString
    getFeedback(idx, args) match {
      case Some(v) => v
      case None => {
        val arg1 = args(1)
        val arg2 = args(2)
        useClassifier match { //use result of case expression
          case None => hints(0)
          case Some((sparmMLInst, modelGen)) => 
            getCache(idx, args, hints) match {
              case None => {
                if(classifyUpFrontAndCache){
                  throw new Exception("Model Not trained") 
                }
                else
                  classify(idx, args, hints) match {
                    case Seq() => pickFromArgs(args) 
                    case x => {
                      val prediction = classToPrimitive( x.head._1 )
                      setCache(idx, args, hints, prediction)
                      pickFromArgs(args, Some(prediction))
                    }
                  }
              }
              case Some(v) => pickFromArgs(args, Some(v))
            }
        }
      }
      
    }
  }
  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]) = {
    useClassifier match { //use result of case expression
      case None => hints(0)
      case Some((sparmMLInst, modelGen)) => {
        val rowidstr = args(0).asString
        val rowid = RowIdPrimitive(rowidstr)
        (if(classifyUpFrontAndCache){
          val predictions = classifyAllPredictions match {
            case None => throw new Exception("Model Not trained")
            case Some(p) => p
          }
          predictions.getOrElse(rowidstr, Seq())
        }
        else{
          classify(idx, args, hints)
        }) match {
          case Seq() => pickFromArgs(args, Some(classToPrimitive("0")))
          case classes => pickFromArgs(args, Some(classToPrimitive(RandUtils.pickFromWeightedList(randomness, classes))))
        } 
      }
    }
  }
  def reason(idx: Int, args: Seq[PrimitiveValue],hints: Seq[PrimitiveValue]): String = {
    val rowid = RowIdPrimitive(args(0).asString)
    getFeedback(idx, args) match {
      case Some(v) =>
        s"${getReasonWho(idx,args)} told me that $resultColumn = $v on row $rowid"
      case None => {
        useClassifier match { //use result of case expression
          case None => s"I used an expressions to pick a value for $resultColumn from columns: ${pickFromCols.mkString(",")}"
          case Some(modelGen) => s"I used a classifier to pick a value for $resultColumn from columns: ${pickFromCols.mkString(",")}"
        }
      }
    }
  }
  def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit = { 
    val rowid = args(0).asString
    setFeedback(idx, args, v)
  }
  def isAcknowledged (idx: Int, args: Seq[PrimitiveValue]): Boolean = {
    hasFeedback(idx, args)
  }
  def hintTypes(idx: Int): Seq[mimir.algebra.Type] = useClassifier match {
    case None => Seq(TAny())
    case Some(x) => Seq() 
  }
   
  def getDomain(idx: Int, args: Seq[PrimitiveValue], hints:Seq[PrimitiveValue]): Seq[(PrimitiveValue,Double)] =  useClassifier match {
    case Some(x) => Seq()//pickFromCols.zipWithIndex.map(colIdx => (args(colIdx._2), 1.0/pickFromCols.length.toDouble))
    case None => Seq((hints(0), 0.0))//pickFromCols.zipWithIndex.map(colIdx => (args(colIdx._2), 1.0/(pickFromCols.length+1).toDouble)) ++ Seq((hints(0), 1.0/(pickFromCols.length+1).toDouble))
  }
  
  def train(db:Database, sparkMLInstModelGen:(() => SparkML,SparkML.SparkModelGenerator)) : (SparkML, SparkML.SparkModel) = {
    val (sparkMLInst, modelGen) = sparkMLInstModelGen
    /*val trainBaseOper = pickFromCols.tail.foldLeft(source.filter(Not(IsNullExpression(Var(pickFromCols.head)))))((init, col) => {
      init.union(Project(Seq(ProjectArg(pickFromCols.head, Var(col)),
        ProjectArg(col, Var(pickFromCols.head))) ++
        pickFromCols.tail.flatMap(otherCol => otherCol match{
          case `col` => None
          case _ => Some(ProjectArg(otherCol, Var(otherCol)))
        }), source.filter(Not(IsNullExpression(Var(col))))))
    })*/
    val trainingQuery = Limit(0, Some(TRAINING_LIMIT), Sort(Seq(SortColumn(Function("random", Seq()), true)), Project(pickFromCols.map(col => ProjectArg(col, Var(col))), source.filter(Not(IsNullExpression(Var(pickFromCols.head)))) )))
    schema = db.typechecker.schemaOf(trainingQuery) :+ (Provenance.rowidColnameBase, TRowId())
    trainingData = db.query(trainingQuery)(res => res.toList.map(row => row.tuple :+ row.provenance))
    classifierModel = Some(modelGen(ModelParams(trainingQuery, db, pickFromCols.head, "skip")))
    sparkMLInstance = Some(sparkMLInst)
    (sparkMLInst(), classifierModel.get)
  }
  
  def classify(idx:Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]) : Seq[(String,Double)] = {
    val (sparkMLInst, classifier) = classifierModel match {
      case None => throw new Exception("Model not trained")//train(useClassifier.get)
      case Some(clsfymodel) => (sparkMLInstance.get(), clsfymodel)
    }
    val predictions = sparkMLInst.extractPredictions(classifier, sparkMLInst.applyModel(classifier,pickFromCols.zip(colTypes) :+ (Provenance.rowidColnameBase, TString()), List(args)))
    //predictions.show()
    predictions.unzip._2
  }
  
  def classifyAll(db:Database) : Unit = {
    val (sparkMLInst, classifier) = classifierModel match {
      case None => train(db, useClassifier.get)
      case Some(clsfymodel) => (sparkMLInstance.get(), clsfymodel)
    }
    /*val classifyAllQuery = Project(pickFromCols.map(col => ProjectArg(col, Var(col))), source)
    val tuples = db.query(classifyAllQuery, mimir.exec.mode.BestGuess)(results => {
      results.toList.map(row => (row.provenance :: row.tuple.toList).toSeq)
    })
    val predictions = sparkMLInst.applyModel(classifier, ("rowid", TString()) +: (pickFromCols.zip(colTypes)), tuples)*/
    val classifyAllQuery = sparkMLInst.getSparkSqlContext().createDataFrame(
      sparkMLInst.getSparkSession().parallelize(trainingData.map( row => {
        Row(row.zip(schema).map(value => sparkMLInst.getNative(value._1, value._2._2) ):_*)
      })), StructType(schema.map(col => StructField(col._1, OperatorTranslation.getSparkType(col._2), true))))
    val predictions = sparkMLInst.applyModel(classifier, classifyAllQuery)
    val predictionMap = sparkMLInst.extractPredictions(classifier, predictions).groupBy(_._1).map { case (k,v) => (k,v.map(_._2))}
    classifyAllPredictions = Some(predictionMap)
    predictionMap.map(mapEntry => {
      setCache(0, Seq(RowIdPrimitive(mapEntry._1)), null, classToPrimitive( mapEntry._2(0)._1))
    })
    db.models.persist(this)
    
  }

  def confidence (idx: Int, args: Seq[PrimitiveValue], hints:Seq[PrimitiveValue]): Double = {
    val rowid = RowIdPrimitive(args(0).asString)
    getFeedback(idx,args) match {
    case Some(v) => 1.0
    case None => 0.0
    }
  }
}
