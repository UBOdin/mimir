package mimir.models;

import scala.util.Random

import mimir.algebra._
import mimir.util._
import mimir.Database
import mimir.ml.spark.SparkML
import mimir.util.TextUtils.Levenshtein
import mimir.ml.spark.SparkML.{SparkModelGeneratorParams => ModelParams }

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
  with NeedsReconnectToDatabase
  with FiniteDiscreteDomain
  with SourcedFeedback
  with ModelCache
{
  
  val TRAINING_LIMIT = 10000
  var classifierModel: Option[SparkML.SparkModel] = None
  var sparkMLInstance: Option[() => SparkML] = None
  var classifyAllPredictions:Option[Map[String, Seq[(String, Double)]]] = None 
  
  @transient var db: Database = null
  
  def getCacheKey(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue] ) : String = args(0).asString
  def getFeedbackKey(idx: Int, args: Seq[PrimitiveValue] ) : String = args(0).asString
 
  private def classToPrimitive(value:String): PrimitiveValue = 
  {
    TextUtils.parsePrimitive(colTypes(0), value)
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
        })
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
                  classifyAll()
                  pickFromArgs(args, getCache(idx, args, hints)) 
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
            case None => {
              classifyAll()
              classifyAllPredictions.get
            }
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
  def hintTypes(idx: Int): Seq[mimir.algebra.Type] = Seq(TAny())
   
  
  def getDomain(idx: Int, args: Seq[PrimitiveValue], hints:Seq[PrimitiveValue]): Seq[(PrimitiveValue,Double)] = Seq((hints(0), 0.0))
  
  def reconnectToDatabase(db: Database) = { 
    this.db = db 
  }
  
  def train(sparkMLInstModelGen:(() => SparkML,SparkML.SparkModelGenerator)) : (SparkML, SparkML.SparkModel) = {
    val (sparkMLInst, modelGen) = sparkMLInstModelGen
    val trainingQuery = Limit(0, Some(TRAINING_LIMIT), Sort(Seq(SortColumn(Function("random", Seq()), true)), Project(pickFromCols.map(col => ProjectArg(col, Var(col))), source)))
    classifierModel = Some(modelGen(ModelParams(trainingQuery, db, pickFromCols.head)))
    sparkMLInstance = Some(sparkMLInst)
    db.models.persist(this)
    (sparkMLInst(), classifierModel.get)
  }
  
  def classify(idx:Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]) : Seq[(String,Double)] = {
    val (sparkMLInst, classifier) = classifierModel match {
      case None => train(useClassifier.get)
      case Some(clsfymodel) => (sparkMLInstance.get(), clsfymodel)
    }
    val predictions = sparkMLInst.extractPredictions(classifier, sparkMLInst.applyModel(classifier, ("rowid", TString()) +: pickFromCols.zip(colTypes), List(args)))
    //predictions.show()
    predictions.unzip._2
  }
  
  def classifyAll() : Unit = {
    val (sparkMLInst, classifier) = classifierModel match {
      case None => train(useClassifier.get)
      case Some(clsfymodel) => (sparkMLInstance.get(), clsfymodel)
    }
    val classifyAllQuery = Project(pickFromCols.map(col => ProjectArg(col, Var(col))), source)
    val tuples = db.query(classifyAllQuery, mimir.exec.mode.BestGuess)(results => {
      results.toList.map(row => (row.provenance :: row.tuple.toList).toSeq)
    })
    val predictions = sparkMLInst.applyModel(classifier, ("rowid", TString()) +: (pickFromCols.zip(colTypes)), tuples)
    val predictionMap = sparkMLInst.extractPredictions(classifier, predictions).groupBy(_._1).map { case (k,v) => (k,v.map(_._2))}
    classifyAllPredictions = Some(predictionMap)
    
    predictionMap.map(mapEntry => {
      setCache(0, Seq(RowIdPrimitive(mapEntry._1)), null, classToPrimitive( mapEntry._2(0)._1))
    })
    
    db.models.persist(this)   
  }
}



