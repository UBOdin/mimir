package mimir.models

import java.io.File
import java.sql.SQLException
import java.util

import mimir.algebra._
import mimir.ctables._
import mimir.exec.ResultIterator
import mimir.util.{TypeUtils,RandUtils}
import mimir.{Analysis, Database}
import moa.classifiers.Classifier
import moa.core.InstancesHeader
import weka.core.{Attribute, DenseInstance, Instance, Instances}
import weka.experiment.{DatabaseUtils, InstanceQueryAdapter}
import mimir.optimizer.InlineVGTerms
import mimir.models._

import scala.collection.JavaConversions._
import scala.util._

object WekaModel
{
  def train(db: Database, name: String, cols: List[String], target:Operator): Map[String,(Model,Int)] = 
  {
    cols.map( (col) => {
      val model = new SimpleWekaModel(s"$name:$col", col, target)
      model.train(db)
      col -> (model, 0)
    }).toMap
  }

  def getAttributesFromIterator(iterator: ResultIterator): util.ArrayList[Attribute] = {
    val attributes = new util.ArrayList[Attribute]()
    iterator.schema.foreach { case (n, t) =>
      (n, t) match {
        case (_, Type.TRowId) => attributes.add(new Attribute(n, null.asInstanceOf[util.ArrayList[String]]))
        case (_, Type.TInt | Type.TFloat) => attributes.add(new Attribute(n))
        case _ => attributes.add(new Attribute(n, null.asInstanceOf[util.ArrayList[String]]))
      }
    }

    attributes
  }

  def decode(db: Database, data: Array[Byte]): Model =
  {
    val in = new java.io.ObjectInputStream(
        new java.io.ByteArrayInputStream(data)
      )
    val name = in.readObject().asInstanceOf[String]
    val colName = in.readObject().asInstanceOf[String]
    val target = 
      db.querySerializer.desanitize(in.readObject().asInstanceOf[Operator])
    val ret = new SimpleWekaModel(name, colName, target)
    ret.numSamples = in.readInt()
    ret.numCorrect = in.readInt()
    ret.learner = weka.core.SerializationHelper.
                    read(in).
                    asInstanceOf[Classifier]
    ret.db = db
    return ret;
  }
}

class SimpleWekaModel(name: String, colName: String, target: Operator)
  extends SingleVarModel(name)
{
  private val TRAINING_LIMIT = 1000
  var numSamples = 0
  var numCorrect = 0
  val colIdx = target.schema.map(_._1).indexOf(colName)
  var learner: Classifier = null
  var db: Database = null

  def train(db:Database)
  {
    this.db = db
    learner = Analysis.getLearner("moa.classifiers.bayes.NaiveBayes")
    val iterator = db.query(target)
    val attributes = WekaModel.getAttributesFromIterator(iterator)
    var data = new Instances("TrainData", attributes, 100)

    var numInstances = 0
    iterator.open()

    /* The second check poses a limit on the learning data and reduces time spent building the lens */
    while(iterator.getNext() && numInstances < TRAINING_LIMIT) {
      // println("ROW: "+iterator.currentRow())
      val instance = new DenseInstance(iterator.numCols)
      instance.setDataset(data)
      for(j <- 0 until iterator.numCols                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       ) {
        iterator.schema(j)._2 match {
          case Type.TInt | Type.TFloat =>
            try {
              instance.setValue(j, iterator(j).asDouble)
            } catch {
              case e: Throwable =>

            }
          case _ =>
            val field = iterator(j)
            if (!field.isInstanceOf[NullPrimitive])
              instance.setValue(j, iterator(j).asString)
        }
      }
      data.add(instance)
      numInstances = numInstances + 1
    }
    iterator.close()
    data.setClassIndex(colIdx)

    learner.setModelContext(new InstancesHeader(data))
    learner.prepareForUse()
    data.foreach(learn(_))
  }

  def learn(dataPoint: Instance) = {
    numSamples += 1;
    if (learner.correctlyClassifies(dataPoint)) {
      numCorrect += 1;
    }
    learner.trainOnInstance(dataPoint);
  }

  private def getAttributesFromIterator(iterator: ResultIterator): util.ArrayList[Attribute] = {
    val attributes = new util.ArrayList[Attribute]()
    iterator.schema.foreach { case (n, t) =>
      (n, t) match {
        case (_, Type.TRowId) => attributes.add(new Attribute(n, null.asInstanceOf[util.ArrayList[String]]))
        case (_, Type.TInt | Type.TFloat) => attributes.add(new Attribute(n))
        case _ => attributes.add(new Attribute(n, null.asInstanceOf[util.ArrayList[String]]))
      }
    }
    attributes
  }

  private def classify(rowid: RowIdPrimitive): List[(Double, Int)] = {
    //println("Classify: "+rowid)
    val rowValues = db.query(
        Select(
          Comparison(Cmp.Eq, RowIdVar(), rowid),
          target
        )
    )
    rowValues.open()
    if (!rowValues.getNext()) {
      throw new SQLException("Invalid Source Data ROWID: " + rowid);
    }
    val row = new DenseInstance(rowValues.numCols)
    val attributes = getAttributesFromIterator(rowValues)
    val data = new Instances("TestData", attributes, 1)
    row.setDataset(data)
    (0 until rowValues.numCols).foreach((col) => {
      val v = rowValues(col)
      if (!v.isInstanceOf[NullPrimitive]) {
        if (v.isInstanceOf[IntPrimitive] || v.isInstanceOf[FloatPrimitive]) {
          row.setValue(col, v.asDouble)
        }
        else {
          row.setValue(col, v.asString)
        }
      }
    })

    rowValues.close()
    learner.getVotesForInstance(row).
      toList.
      zipWithIndex.
      filter(_._1 > 0)
  }

  private def classToPrimitive(classIdx: Int): PrimitiveValue = 
  {
    val att = learner.getModelContext.attribute(colIdx)
    if (att.isString)
      StringPrimitive(att.value(classIdx))
    else if (att.isNumeric)
      IntPrimitive(classIdx)
    else
      throw new SQLException("Unknown type")
  }

  def varType(argTypes: List[Type.T]): Type.T = 
  {
    val att = learner.getModelContext.attribute(colIdx)
    if(att.isString){ Type.TString }
    else if(att.isNumeric){ Type.TInt }
    else { 
      throw new SQLException("Unknown type")
    }
  }
  def bestGuess(args: List[PrimitiveValue]): PrimitiveValue =
  {
    val classes = classify(args(0).asInstanceOf[RowIdPrimitive])
    val res = if (classes.isEmpty) { 0 } 
              else { classes.maxBy(_._1)._2 }
    classToPrimitive(res)
  }
  def sample(randomness: Random, args: List[PrimitiveValue]): PrimitiveValue = 
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
  def reason(args: List[Expression]): String = 
  {
    val classes = classify(args(0).asInstanceOf[RowIdPrimitive])
    val total:Double = classes.map(_._1).fold(0.0)(_+_)
    val res = if (classes.isEmpty) { (0.0, 0) } 
              else { classes.maxBy(_._1) }
    val elem = classToPrimitive(res._2)
    "I used a classifier to guess that "+colName+"="+elem+" on row "+
    args(0)+" ("+res._1+" out of "+total+" votes)"

  }

  override def serialize: (Array[Byte], String) =
  {
    val bytes = new java.io.ByteArrayOutputStream()
    val objects = new java.io.ObjectOutputStream(bytes)
    objects.writeObject(name)
    objects.writeObject(colName)
    objects.writeObject(db.querySerializer.sanitize(target))
    objects.writeInt(numSamples)
    objects.writeInt(numCorrect)
    objects.writeObject(learner)
    weka.core.SerializationHelper.write(objects,learner)

    (bytes.toByteArray, "WEKA")
  }
}