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
import mimir.statistics.FuncDep

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.util._

object FuncDepModel
{
  val logger = Logger(org.slf4j.LoggerFactory.getLogger(getClass.getName))

  def train(db: Database, name: String, cols: Seq[String], query:Operator): Map[String,(Model,Int,Seq[Expression])] =
  {
    cols.map( (col) => {
      val modelName = s"$name:$col"
      var sourceCol = 0
      val model =
        db.models.getOption(modelName) match {
          case Some(model) => model
          case None => {
            val model = new SimpleFuncDepModel(modelName, col, query)
            sourceCol = model.train(db)
            db.models.persist(model)
            model
          }
        }
        val sourceColName=query.columnNames(sourceCol)
        val tableName = QueryNamer(query)
      col -> (
        model,                         // The model for the column
        0,                             // The model index of the column's replacement variable
        db.query(query.project(sourceColName)){_.tuples}.flatten  // 'Hints' for the model -- The dependent column values
      )
    }).toMap
  }
}

@SerialVersionUID(1001L)
class SimpleFuncDepModel(name: String, colName: String, query: Operator)
  extends Model(name)
  with NeedsReconnectToDatabase
{
  val colIdx:Int = query.columnNames.indexOf(colName)
  val feedback = scala.collection.mutable.Map[String,PrimitiveValue]()
  var inEdgeCol = ""
  val tableName = QueryNamer(query)
  var maxStrength = 0.0
  var dependencyMap = scala.collection.immutable.Map[PrimitiveValue,PrimitiveValue]() //inEdge->col
  var weightedList = IndexedSeq[(PrimitiveValue,Double)]()
  /**
   * The database is marked transient.  We use the @NeedsReconnectToDatabase trait to
   * ask the deserializer to re-link us.
   */
  @transient var db: Database = null

  /**
   * When the model is created, learn associations from the existing data.
   */
  def train(db:Database) : Int =
  {
    val fd = new FuncDep()
    fd.buildEntities(db, query, tableName)
    //  use egde strength to choose incoming edges
    var inedgeArr0 : Array[(Int,Int)] = new Array[(Int,Int)](0)
    val inedgeArr = fd.fdGraph.getInEdges(colIdx).toArray(inedgeArr0)
    val edgeMap = fd.edgeTable.map{case (a,b,c)=> (a,b)->c}.toMap
    var inEdge = 0
    for((a,b)<-inedgeArr) {
      val str = edgeMap.get((a,b)) match {
        case Some(v) => v
        case None => -1
      }
      if(str>=maxStrength && a!=(-1)) {
        maxStrength = str
        inEdge = a
      }
    }
    inEdgeCol = query.columnNames(inEdge)
    val results = db.query(Aggregate(Seq(Var(inEdgeCol),Var(colName)),Seq(AggFunction("COUNT", false, Seq(), "count1")),query).project(inEdgeCol,colName,"count1")){_.tuples}
                      .filterNot(row => row(1).isInstanceOf[NullPrimitive]).sortBy(_(2).asInt)
    dependencyMap = results.map(arr => arr(0) -> arr(1)).toMap
    weightedList = results.map(row => (row(1),row(2).asDouble))
    this.db = db
    inEdge
  }

  def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit =
  {
    val rowid = args(0).asString
    feedback(rowid) = v
  }

  def isAcknowledged(idx: Int, args: Seq[PrimitiveValue]): Boolean =
    feedback contains(args(0).asString)

  def guessInputType: Type =
    db.bestGuessSchema(query)(colIdx)._2

  def argTypes(idx: Int): Seq[Type] = List(TRowId())
  def hintTypes(idx: Int) = db.typechecker.schemaOf(query).map(_._2)

  def varType(idx: Int, args: Seq[Type]): Type = guessInputType

  def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): PrimitiveValue =
  {
    val rowid = RowIdPrimitive(args(0).asString)
    val rowN = args(0).asInt-1
    val inVal = hints(rowN)
    feedback.get(rowid.asString) match {
      case Some(v) => v
      case None =>
        if(inVal.isInstanceOf[NullPrimitive]){
          NullPrimitive()
        }
        else {
          dependencyMap.getOrElse(inVal,NullPrimitive())
        }
    }
  }
  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): PrimitiveValue =
  {
    val rowid = RowIdPrimitive(args(0).asString)
    if(weightedList.isEmpty){
      NullPrimitive()
    }
    else{
      RandUtils.pickFromWeightedList(randomness, weightedList)
    }
  }
  def reason(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): String =
  {
    val rowid = RowIdPrimitive(args(0).asString)
    val rowN = args(0).asInt-1
    val inVal = hints(rowN)
    feedback.get(rowid.asString) match {
      case Some(v) =>
        s"You told me that $name.$colName = $v on row $rowid"
      case None =>
        if(inVal.isInstanceOf[NullPrimitive]) {
          "I couldn't make a guess"
        } else {
          val elem = dependencyMap.getOrElse(inVal,NullPrimitive())
          if (elem.isInstanceOf[NullPrimitive]){
            s"I detected a functional dependency with strength $maxStrength but could not guess a value"
          }
          else {
            s"I detected a functional dependency with strength $maxStrength to guess that $name.$colName = $elem on row $rowid"
          }
        }
    }
  }

  /**
   * Re-populate transient fields after being woken up from serialization
   */
  def reconnectToDatabase(db: Database) = {
    this.db = db
  }

  def confidence (idx: Int, args: Seq[PrimitiveValue], hints:Seq[PrimitiveValue]) : Double = {
    val rowid = RowIdPrimitive(args(0).asString)
    val rowN = args(0).asInt-1
    val inVal = hints(rowN)
    feedback.get(rowid.asString) match {
      case Some(v) => 1.0
      case None =>
      if(inVal.isInstanceOf[NullPrimitive]) {
        0
      } else {
        val elem = dependencyMap.getOrElse(inVal,NullPrimitive())
        if (elem.isInstanceOf[NullPrimitive]) {
          0
        }
        else {
          maxStrength
        }
      }
    }
  }

}
