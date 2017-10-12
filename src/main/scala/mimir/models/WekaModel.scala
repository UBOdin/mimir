package mimir.models

import java.io.File
import java.sql.SQLException
import java.util
import com.typesafe.scalalogging.slf4j.Logger

import mimir.algebra._
import mimir.ctables._
import mimir.util.{RandUtils,TextUtils,TimeUtils}
import mimir.{Analysis, Database}
import moa.core.InstancesHeader
import weka.core.{Attribute, DenseInstance, Instance, Instances}
import weka.experiment.{DatabaseUtils, InstanceQueryAdapter}
import weka.classifiers.{Classifier, UpdateableClassifier}
import weka.classifiers.bayes.{NaiveBayesMultinomial,NaiveBayesMultinomialUpdateable,NaiveBayesMultinomialText}
import mimir.models._

import scala.collection.JavaConversions._
import scala.util._

object WekaModel
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
            val model = new SimpleWekaModel(modelName, col, query)
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

  def getStringAttribute(db: Database, col: String, query: Operator): Attribute =
  {
    val tokens: Set[String] =
      db.query(
        Limit(0, Some(TOKEN_LIMIT),
          Project(List(ProjectArg("V", Var(col))), query)
        )
      ) { result =>
        result.foldLeft(Set[String]()) { (ret, curr) => 
          if(!curr(0).isInstanceOf[NullPrimitive]){
            ret + curr(0).asString 
          } else { ret }
        }
      }

    val tokenList = new java.util.ArrayList(tokens)
    java.util.Collections.sort(tokenList)
    new Attribute(col, tokenList)
  }

  def getAttributes(db: Database, query: Operator): Seq[Attribute] =
  {
    db.typechecker.schemaOf(query).map({
      // case (col, TInt() | TFloat() | TDate()) => new Attribute(col)
      case (col, _) => getStringAttribute(db, col, query)
    })
  }
}

@SerialVersionUID(1001L)
class SimpleWekaModel(name: String, colName: String, query: Operator)
  extends Model(name) 
  with NeedsReconnectToDatabase 
  with SourcedFeedback
{
  var numSamples = 0
  var numCorrect = 0
  val colIdx:Int = query.columnNames.indexOf(colName)
  var attributeMeta: java.util.ArrayList[Attribute] = null
  
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

  def getFeedbackKey(idx: Int, args: Seq[PrimitiveValue] ) : String = args(0).asString
 
  def rowToInstance(row: Seq[PrimitiveValue], dataset: Instances): DenseInstance =
  {
    val instance = new DenseInstance(row.size)
    instance.setDataset(dataset)
    for( (field, idx) <- row.zipWithIndex ){
      if(!field.isInstanceOf[NullPrimitive]){
        val attr = attributeMeta(idx) 
        if(attr.isNumeric){
          WekaModel.logger.trace(s"Number: $idx -> $field")
          field match {
            case DatePrimitive(y, m, d) => instance.setValue(idx, y * 1000 + m * 10 + d)
            case _ => instance.setValue(idx, field.asDouble)
          }
        } else if(attr.isNominal) {
          val fieldString = field.asString
          val enumId = attr.indexOfValue(fieldString)
          WekaModel.logger.trace(s"Nominal: $idx -> $field ($enumId)")
          if(enumId >= 0){
            instance.setValue(idx, fieldString)            
          } else {
            WekaModel.logger.debug(s"Undefined Nominal Class ($idx): $field")
          }
        } else {
          throw new RAException("Invalid attribute type")
        }
      } else {
        WekaModel.logger.trace(s"NULL: $idx")
      }
    }
    return instance
  }

  /**
   * When the model is created, learn associations from the existing data.
   */
  def train(db:Database)
  {
    this.db = db
    TimeUtils.monitor(s"Train $name.$colName", WekaModel.logger.info(_)){
      val trainingQuery = Limit(0, Some(WekaModel.TRAINING_LIMIT), query)
      WekaModel.logger.debug(s"TRAINING ON: \n$trainingQuery")
      db.query(trainingQuery) { iterator => 
        attributeMeta = new java.util.ArrayList(WekaModel.getAttributes(db, query))
        val data = new Instances("TrainData", attributeMeta, WekaModel.TRAINING_LIMIT)

        for( row <- iterator.take(WekaModel.TRAINING_LIMIT) ){
          WekaModel.logger.trace(s"ROW: $row")
          data.add(rowToInstance(row.tuple, data))
        }
        data.setClassIndex(colIdx)

        // val model = new NaiveBayesMultinomialUpdateable()
        // val model = new NaiveBayesMultinomial()
        val model = new NaiveBayesMultinomialText()
        hackToPreventGUILaunch()
        model.buildClassifier(data)
        learner = model
      }
    }
  }

  def hackToPreventGUILaunch(){
    val f = classOf[java.awt.GraphicsEnvironment].getDeclaredField("headless");
    f.setAccessible(true);
    f.set(null, true);
  }
  
  def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit =
  {
    setFeedback(idx, args, v)
  }

  def isAcknowledged(idx: Int, args: Seq[PrimitiveValue]): Boolean =
    hasFeedback(idx, args)

  /**
   * Improve the model with one single data point
   */
  def learn(dataPoint: Instance) = {
    learner.updateClassifier(dataPoint)
  }

  private def classify(rowid: RowIdPrimitive, rowValueHints: Seq[PrimitiveValue]): Seq[(Double, Int)] = {
    val rowValues = 
      if(rowValueHints.isEmpty){
        db.query(
            Select(
              Comparison(Cmp.Eq, RowIdVar(), rowid),
              query
            )
        ) { results =>
          if (!results.hasNext()) {
            throw new SQLException("Invalid Source Data for Weka Model on ROWID: " + rowid);
          }
          results.next.tuple
        }
      } else { rowValueHints }
    
    val data = new Instances("TestData", attributeMeta, 1)
    val row = rowToInstance(rowValues, data)

    val votes = learner.distributionForInstance(row).toSeq

    WekaModel.logger.debug(s"VOTES (${votes.size}): ${votes.take(5)}")
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
  def hintTypes(idx: Int) = db.typechecker.schemaOf(query).map(_._2)

  def varType(idx: Int, args: Seq[Type]): Type = guessInputType
  
  def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): PrimitiveValue =
  {
    val rowid = RowIdPrimitive(args(0).asString)
    getFeedback(idx, args) match {
      case Some(v) => v
      case None => 
        val classes = classify(rowid, hints)
        val res = if (classes.isEmpty) { 0 } 
                  else { classes.maxBy(_._1)._2 }
        classToPrimitive(res)
    }
  }
  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): PrimitiveValue = 
  {
    val rowid = RowIdPrimitive(args(0).asString)
    val classes = classify(rowid, hints)
    val res = if (classes.isEmpty) { 0 }
              else {
                RandUtils.pickFromWeightedList(
                  randomness,
                  classes.map(x => (x._2,x._1))
                )
              }
    classToPrimitive(res)
  }
  def reason(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): String = 
  {
    val rowid = RowIdPrimitive(args(0).asString)
    getFeedback(idx, args) match {
      case Some(v) =>
        s"${getReasonWho(idx,args)} told me that $name.$colName = $v on row $rowid"
      case None => 
        val classes = classify(rowid.asInstanceOf[RowIdPrimitive], hints)
        val total:Double = classes.map(_._1).fold(0.0)(_+_)
        if (classes.isEmpty) { 
          val elem = classToPrimitive(0)
          s"The classifier isn't able to make a guess about $name.$colName, so I'm defaulting to $elem"
        } else {
          val choice = classes.maxBy(_._1)
          val elem = classToPrimitive(choice._2)
          s"I used a classifier to guess that ${name.split(":")(0)}.$colName = $elem on row $rowid (${choice._1} out of ${total} votes)"
        }
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
    WekaModel.logger.debug(s"Serialized Learner is ${serializedLearner.size} bytes")
    val ret = super.serialize()
    serializedLearner = null
    return ret
  }

  def confidence (idx: Int, args: Seq[PrimitiveValue], hints:Seq[PrimitiveValue]) : Double = {
    val rowid = RowIdPrimitive(args(0).asString)
    getFeedback(idx,args) match {
      case Some(v) => 1.0
      case None =>
        val classes = classify(rowid, hints)
        if (classes.isEmpty) { 0.0 }
        else { classes.maxBy(_._1)._1/classes.map(_._1).sum }
    }
  }
}
