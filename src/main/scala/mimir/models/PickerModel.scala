package mimir.models;

import scala.util.Random

import mimir.algebra._
import mimir.util._
import mimir.Database
import mimir.ml.spark.{MultiClassClassification, ReverseIndexRecord}


/**
 * A model representing a key-repair choice.
 * 
 * The index is ignored.
 * The one argument is a value for the key.  
 * The return value is an integer identifying the ordinal position of the selected value, starting with 0.
 */
@SerialVersionUID(1000L)
class PickerModel(override val name: String, resultColumn:String, pickFromCols:Seq[String], colTypes:Seq[Type], useClassifier:Option[MultiClassClassification.ClassifierModelGenerator], classifyUpFrontAndCache:Boolean, source: Operator) 
  extends Model(name) 
  with Serializable
  with NeedsReconnectToDatabase
  with FiniteDiscreteDomain
{
  
  val feedback = scala.collection.mutable.Map[String,PrimitiveValue]()
  val classificationCache = scala.collection.mutable.Map[String,PrimitiveValue]()
  val TRAINING_LIMIT = 10000
  var classifierModel: Option[MultiClassClassification.ClassifierModel] = None
  
  @transient var db: Database = null
  
    
  def argTypes(idx: Int) = {
      Seq(TRowId()).union(colTypes)
  }
  def varType(idx: Int, args: Seq[Type]) = colTypes(idx)
  def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]  ) = {
    val rowid = args(0).asString
    feedback.get(rowid) match {
      case Some(v) => v
      case None => {
        val arg1 = args(1)
        val arg2 = args(2)
        useClassifier match { //use result of case expression
          case None => hints(0)
          case Some(modelGen) => 
            classificationCache.get(rowid) match {
              case None => {
                if(classifyUpFrontAndCache){
                  classifyAll()
                  classificationCache.get(rowid) match {
                    case None => {
                      pickFromCols.zipWithIndex.foldLeft(NullPrimitive():PrimitiveValue)( (init, elem) => init match {
                        case NullPrimitive() => args(elem._2+1)
                        case x => x
                      })
                    }
                    case Some(v) => v
                  }
                }
                else
                  classify(idx, args, hints)
              }
              case Some(v) => v
            }
        }
      }
      
    }
  }
  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]) = {
    val rowid = RowIdPrimitive(args(0).asString)
    this.db = db
    val sampleChoices = scala.collection.mutable.Map[String,Seq[PrimitiveValue]]()
   db.query(Select(Comparison(Cmp.Eq, RowIdVar(), rowid), source))( iterator => {
    val pickCols = iterator.schema.map(_._1).zipWithIndex.flatMap( si => {
      if(pickFromCols.contains(si._1))
        Some((pickFromCols.indexOf(si._1), si))
      else
         None
    }).sortBy(_._1).unzip._2
    while(iterator.hasNext() ) {
      val row = iterator.next()
      sampleChoices(row.provenance.asString) = pickCols.map(si => {
        row(si._2)
      })
    }
   })
    RandUtils.pickFromList(randomness, sampleChoices(rowid.asString))
  }
  def reason(idx: Int, args: Seq[PrimitiveValue],hints: Seq[PrimitiveValue]): String = {
    val rowid = RowIdPrimitive(args(0).asString)
    feedback.get(rowid.asString) match {
      case Some(v) =>
        s"You told me that $resultColumn = $v on row $rowid"
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
    feedback(rowid) = v
  }
  def isAcknowledged (idx: Int, args: Seq[PrimitiveValue]): Boolean = {
    feedback contains(args(0).asString)
  }
  def hintTypes(idx: Int): Seq[mimir.algebra.Type] = Seq(TAny())
   
  
  def getDomain(idx: Int, args: Seq[PrimitiveValue], hints:Seq[PrimitiveValue]): Seq[(PrimitiveValue,Double)] = Seq((hints(0), 0.0))
  
  def reconnectToDatabase(db: Database) = { 
    this.db = db 
  }
  
  def train(modelGen:MultiClassClassification.ClassifierModelGenerator) : MultiClassClassification.ClassifierModel = {
    val trainingQuery = Limit(0, Some(TRAINING_LIMIT), Sort(Seq(SortColumn(Function("random", Seq()), true)), Project(pickFromCols.map(col => ProjectArg(col, Var(col))), source)))
    classifierModel = Some(modelGen(trainingQuery, db, pickFromCols.head))
    db.models.persist(this)
    classifierModel.get
  }
  
  def classify(idx:Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]) : PrimitiveValue = {
    val classifier = classifierModel match {
      case None => train(useClassifier.get)
      case Some(clsfymodel) => clsfymodel
    }
    val predictions = MultiClassClassification.extractPredictions(classifier, MultiClassClassification.classify(classifier, pickFromCols.zip(colTypes), List(args)))
    //predictions.show()
    predictions match {
      case Seq() => {
        pickFromCols.zipWithIndex.foldLeft(NullPrimitive():PrimitiveValue)( (init, elem) => init match {
          case NullPrimitive() => args(elem._2+1)
          case x => x
        })
      }
      case x => {
        val prediction = mimir.parser.ExpressionParser.expr( x.head.head._1 ).asInstanceOf[PrimitiveValue]
        classificationCache(args(0).asString) = prediction
        prediction
      }
    }
    
  }
  
  def classifyAll() : Unit = {
    val classifier = classifierModel match {
      case None => train(useClassifier.get)
      case Some(clsfymodel) => clsfymodel
    }
    val classifyAllQuery = Project(pickFromCols.map(col => ProjectArg(col, Var(col))), source)
    val tuples = db.query(classifyAllQuery, mimir.exec.mode.BestGuess)(results => {
      results.toList.map(row => (row.provenance :: row.tuple.toList).toSeq)
    })
    val predictions = MultiClassClassification.classify(classifier, pickFromCols.zip(colTypes), tuples)
    val sqlContext = MultiClassClassification.getSparkSqlContext()
    import sqlContext.implicits._  
    
    //method 1: window, rank, and drop
    /*import org.apache.spark.sql.functions.row_number
    import org.apache.spark.sql.expressions.Window
    val w = Window.partitionBy($"rowid").orderBy($"probability".desc)
    val topPredictionsForEachRow = predictions.withColumn("rn", row_number.over(w)).where($"rn" === 1).drop("rn")*/
    
    //method 2: group and first
    import org.apache.spark.sql.functions.first
    val topPredictionsForEachRow = predictions.sort($"rowid", $"probability".desc).groupBy($"rowid").agg(first(predictions.columns.tail.head).alias(predictions.columns.tail.head), predictions.columns.tail.tail.map(col => first(col).alias(col) ):_*) 
   
    topPredictionsForEachRow.select("rowid", "predictedLabel").collect().map(row => {
      classificationCache(row.getString(0)) = mimir.parser.ExpressionParser.expr( row.getString(1) ).asInstanceOf[PrimitiveValue]
    })
    db.models.persist(this)   
  }
}



