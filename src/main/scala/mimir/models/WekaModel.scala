package mimir.models

import java.io.File
import java.sql.SQLException
import java.util
import com.typesafe.scalalogging.slf4j.Logger

import mimir.algebra._
import mimir.ctables._
import mimir.exec.ResultIterator
import mimir.util.{RandUtils,TextUtils}
import mimir.{Analysis, Database}
import moa.core.InstancesHeader
import weka.core.{Attribute, DenseInstance, Instance, Instances}
import weka.experiment.{DatabaseUtils, InstanceQueryAdapter}
import weka.classifiers.{Classifier, UpdateableClassifier}
import weka.classifiers.bayes.{NaiveBayesMultinomialUpdateable,NaiveBayesMultinomialText}
import mimir.optimizer.InlineVGTerms
import mimir.models._

import scala.collection.JavaConversions._
import scala.util._

object WekaModel
{
  val logger = Logger(org.slf4j.LoggerFactory.getLogger(getClass.getName))

  def train(db: Database, name: String, cols: Seq[String], query:Operator): Map[String,(Model,Int)] = 
  {
    cols.map( (col) => {
      val model = new SimpleWekaModel(s"$name:$col", col, query)
      model.train(db)
      col -> (model, 0)
    }).toMap
  }

  def getStringAttribute(db: Database, col: String, query: Operator): Attribute =
  {
    val tokens =
      db.query(Project(List(ProjectArg("V", Var(col))), query)).
        foldRows[Set[String]](Set[String](), (ret, curr) => { 
          if(!curr(0).isInstanceOf[NullPrimitive]){
            ret + curr(0).asString 
          } else { ret }
        })

    val tokenList = new java.util.ArrayList(tokens)
    java.util.Collections.sort(tokenList)
    new Attribute(col, tokenList)
  }

  def getAttributes(db: Database, query: Operator): Seq[Attribute] =
  {
    query.schema.map({
      // case (col, TInt() | TFloat()) => new Attribute(col)
      case (col, _) => getStringAttribute(db, col, query)
    })
  }
}

@SerialVersionUID(1000L)
class SimpleWekaModel(name: String, colName: String, query: Operator)
  extends Model(name) 
  with NeedsReconnectToDatabase 
{
  private val TRAINING_LIMIT = 10000
  var numSamples = 0
  var numCorrect = 0
  val colIdx:Int = query.schema.map(_._1).indexOf(colName)
  var attributeMeta: java.util.ArrayList[Attribute] = null
  val givens = scala.collection.mutable.Map[String,PrimitiveValue]()

  /**
   * The actual Weka model itself.  @SimpleWekaModel is just a wrapper around a 
   * Weka @Classifier object.  Due to silliness in Weka, @Classifier itself is not
   * serializable, so we mark it transient and use a simple serialization hack.
   * See serializedLearner below.
   */
  @transient var learner: Classifier with UpdateableClassifier = null
  /**
   * Due to silliness in Weka, @Classifier itself is not serializable, and we need
   * to use Weka's internal @SerializationHelper object to do the actual serialization
   * 
   * The fix is to interpose on the serialization/deserialization process.  First, 
   * when the model is serialized in serialize(), we serialize Classifier into this
   * byte array.  Second, when the @NeedsReconnectToDatabase trait fires --- that is,
   * when the model is re-linked to the database, we take the opportunity to deserialize
   * this field.  
   * 
   * To save space, serializedLearner is kept null, except when the object is being
   * serialized.
   */
  var serializedLearner: Array[Byte] = null
  /**
   * The database is marked transient.  We use the @NeedsReconnectToDatabase trait to
   * ask the deserializer to re-link us.
   */
  @transient var db: Database = null

  /**
   * When the model is created, learn associations from the existing data.
   */
  def train(db:Database)
  {
    this.db = db
    val iterator = db.query(query)

    attributeMeta = new java.util.ArrayList(WekaModel.getAttributes(db, query))
    var data = new Instances("TrainData", attributeMeta, TRAINING_LIMIT)

    var numInstances = 0
    iterator.open()

    /* The second check poses a limit on the learning data and reduces time spent building the lens */
    while(iterator.getNext() && numInstances < TRAINING_LIMIT) {
      WekaModel.logger.trace(s"ROW: ${iterator.currentRow()}")
      val instance = new DenseInstance(iterator.numCols)
      instance.setDataset(data)
      for(j <- 0 until iterator.numCols) {
        val field = iterator(j)
        if(!field.isInstanceOf[NullPrimitive]){
          val attr = attributeMeta(j) 
          if(attr.isNumeric){
            instance.setValue(j, field.asDouble)
          } else if(attr.isNominal) {
            instance.setValue(j, field.asString)
          } else {
            throw new RAException("Invalid attribute type")
          }
        }
      }
      data.add(instance)
      numInstances = numInstances + 1
    }
    iterator.close()
    data.setClassIndex(colIdx)

    // val model = new NaiveBayesMultinomialUpdateable()
    val model = new NaiveBayesMultinomialText()
    model.buildClassifier(data)
    learner = model
  }

  def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit =
  {
    givens(args(0).asString) = v
    val row = db.query(
      Select(
        Comparison(Cmp.Eq, RowIdVar(), args(0)),
        query
      )
    ).allRows.head

  }

  def isAcknowledged(idx: Int, args: Seq[PrimitiveValue]): Boolean =
    givens contains(args(0).asString)

  /**
   * Improve the model with one single data point
   */
  def learn(dataPoint: Instance) = {
    learner.updateClassifier(dataPoint)
  }

  private def classify(rowid: RowIdPrimitive): Seq[(Double, Int)] = {
    //println("Classify: "+rowid)
    val rowValues = db.query(
        Select(
          Comparison(Cmp.Eq, RowIdVar(), rowid),
          query
        )
    )
    rowValues.open()
    if (!rowValues.getNext()) {
      throw new SQLException("Invalid Source Data ROWID: " + rowid);
    }
    val row = new DenseInstance(rowValues.numCols)
    val data = new Instances("TestData", attributeMeta, 1)
    row.setDataset(data)
    WekaModel.logger.debug(s"CLASSIFY: ${rowValues.currentRow}")
    (0 until rowValues.numCols).foreach((col) => {
      val v = rowValues(col)
      if (!v.isInstanceOf[NullPrimitive] && (col != colIdx)) {
        // if (v.isInstanceOf[IntPrimitive] || v.isInstanceOf[FloatPrimitive]) {
        //   logger.trace(s"Double: $col -> $v")
        //   row.setValue(col, v.asDouble)
        // }
        // else {
          WekaModel.logger.trace(s"String: $col -> $v")
          row.setValue(col, v.asString)
        // }
      } else {
        WekaModel.logger.trace(s"NULL: $col")
      }
    })

    rowValues.close()
    val votes = learner.distributionForInstance(row).toSeq

    WekaModel.logger.debug(s"VOTES: $votes")
    votes.
      zipWithIndex.
      filter(_._1 > 0)
  }

  private def classToPrimitive(classIdx: Int): PrimitiveValue = 
  {
    var str:String = ""
    try{
      str = attributeMeta(colIdx).value(classIdx)
    }
    catch{
      case e:IndexOutOfBoundsException =>{
        throw ModelException("Column " + colName + " May be all null, Model could not be built over this column")
      }
    }
//    println("Attribute String: " + str)
    TextUtils.parsePrimitive(guessInputType, str)
  }

  def guessInputType: Type =
    db.bestGuessSchema(query)(colIdx)._2

  def argTypes(idx: Int): Seq[Type] = List(TRowId())

  def varType(idx: Int, args: Seq[Type]): Type = guessInputType
  
  def bestGuess(idx: Int, args: Seq[PrimitiveValue]): PrimitiveValue =
  {
    val classes = classify(args(0).asInstanceOf[RowIdPrimitive])
    val res = if (classes.isEmpty) { 0 } 
              else { classes.maxBy(_._1)._2 }
    classToPrimitive(res)
  }
  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue]): PrimitiveValue = 
  {
    val classes = classify(args(0).asInstanceOf[RowIdPrimitive])
    val res = if (classes.isEmpty) { 0 }
              else {
                RandUtils.pickFromWeightedList(
                  randomness,
                  classes.map(x => (x._2,x._1))
                )
              }
    classToPrimitive(res)
  }
  def reason(idx: Int, args: Seq[PrimitiveValue]): String = 
  {
    val classes = classify(args(0).asInstanceOf[RowIdPrimitive])
    val total:Double = classes.map(_._1).fold(0.0)(_+_)
    if (classes.isEmpty) { 
      val elem = classToPrimitive(0)
      s"The classifier isn't willing to make a guess about $colName, so I'm defaulting to $elem"
    } else {
      val choice = classes.maxBy(_._1)
      val elem = classToPrimitive(choice._2)
      s"I used a classifier to guess that $colName = $elem on row ${args(0)} (${choice._1} out of ${total} votes)"
    }
  }

  /**
   * Re-populate transient fields after being woken up from serialization
   */
  def reconnectToDatabase(db: Database): Unit = {
    this.db = db
    val bytes = new java.io.ByteArrayInputStream(serializedLearner)
    learner = weka.core.SerializationHelper.read(bytes).asInstanceOf[Classifier with UpdateableClassifier]
    serializedLearner = null
  }

  /**
   * Interpose on the serialization pipeline to safely serialize the
   * Weka classifier (which doesn't play nicely with ObjOutputStream)
   */
  override def serialize: (Array[Byte], String) =
  {
    val bytes = new java.io.ByteArrayOutputStream()
    weka.core.SerializationHelper.write(bytes,learner)
    serializedLearner = bytes.toByteArray()
    val ret = super.serialize()
    serializedLearner = null
    return ret
  }
}
