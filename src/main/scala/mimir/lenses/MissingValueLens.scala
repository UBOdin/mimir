package mimir.lenses

import java.io.File
import java.sql._
import java.util

import mimir.algebra._
import mimir.ctables._
import mimir.exec.ResultIterator
import mimir.util.TypeUtils
import mimir.{Analysis, Database}
import moa.classifiers.Classifier
import moa.core.InstancesHeader
import weka.core.{Attribute, DenseInstance, Instance, Instances}
import weka.experiment.{DatabaseUtils, InstanceQueryAdapter}
import mimir.optimizer.InlineVGTerms

import scala.collection.JavaConversions._
import scala.util._
;

class MissingValueLens(name: String, args: List[Expression], source: Operator)
  extends Lens(name, args, source)
  with InstanceQueryAdapter {
  var orderedSourceSchema: List[(String, Type)] = null
  val keysToBeCleaned = args.map(Eval.evalString(_).toUpperCase)
  var models: List[SingleVarModel] = null
  var model: Model = null
  var db: Database = null

  def sourceSchema() = {
    if (orderedSourceSchema == null) {
      orderedSourceSchema =
        source.schema.map(_ match { case (n, t) => (n.toUpperCase, t) })
    }
    orderedSourceSchema
  }

  def schema(): List[(String, Type)] = sourceSchema()

  def allKeys() = {
    sourceSchema.map(_._1)
  }

  def view: Operator = {
    Project(
      allKeys().
        map((k) => {
        val v = keysToBeCleaned.indexOf(k);
        if (v >= 0) {
          val u = allKeys().indexOf(k)
          ProjectArg(k,
            Conditional(
              mimir.algebra.IsNullExpression(Var(k)),
              rowVar(u),
              Var(k)
            ))
        } else {
          ProjectArg(k, Var(k))
        }
      }),
      source
    )
  }

  def build(db: Database): Unit = {
    build(db, false)
  }

  def build(db: Database, loading: Boolean): Unit = {
    this.db = db
    models =
      InlineVGTerms.optimize(source).schema.map(
        _ match { case (n, t) => (keysToBeCleaned.indexOf(n), t) }
      ).zipWithIndex.map( (x) => x._1 match { case (idx, t) =>
        if (idx < 0) {
          new NoOpModel(t).asInstanceOf[SingleVarModel]
        } else {
          val m = new MissingValueModel(this, name+"_"+x._2, t)
          val classIndex = allKeys().indexOf(keysToBeCleaned.get(idx))
          if(loading) {
            val path = java.nio.file.Paths.get(
              db.lenses.serializationFolderPath.toString,
              name+"_"+x._2
            ).toString
            val classifier = weka.core.SerializationHelper.read(path).asInstanceOf[Classifier]
            m.init(classifier, classIndex)
          } else {
            m.init(source, classIndex)
          }
          m.asInstanceOf[SingleVarModel]
        }
      })
    model = new IndependentVarsModel(models)
  }

  override def save(db: Database): Unit = {
    models.indices.foreach( (i) =>
      models(i) match {
        case model: MissingValueModel =>
          val classifier = model.getLearner
          val path = java.nio.file.Paths.get(
            db.lenses.serializationFolderPath.toString,
            name+"_"+i
          ).toString
          val file = new File(path)
          file.getParentFile.mkdirs()
          weka.core.SerializationHelper.write(path, classifier)

        case _ =>

      }
    )
  }

  override def load(db: Database): Unit = {
    try {
      build(db, true)
    } catch {
      case e: Throwable =>
        println(name+": LOAD LENS FAILED, REBUILDING...")
        build(db, false)
        save(db)
    }
  }

  def lensType = "MISSING_VALUE"

  ////// Weka's InstanceQueryAdapter interface
  def attributeCaseFix(colName: String) = colName

  def getDebug = false
                                                                                                                                                                                                                                               def getSparseData = false

  def translateDBColumnType(t: String) = {
    t.toUpperCase match {
      case "STRING" => DatabaseUtils.STRING;
      case "VARCHAR" => DatabaseUtils.STRING;
      case "TEXT" => DatabaseUtils.TEXT;
      case "INT" => DatabaseUtils.LONG;
      case "DECIMAL" => DatabaseUtils.DOUBLE;
    }
  }
}

class MissingValueModel(lens: MissingValueLens, name: String, val varType: Type)
  extends SingleVarModel() {
  var learner: Classifier =
    Analysis.getLearner("moa.classifiers.bayes.NaiveBayes")
  var data: Instances = null
  var numCorrect = 0
  var numSamples = 0
  var cIndex = -1
  private val TRAINING_LIMIT = 100

  def reason(args: List[Expression]): String=
    "I made a best guess estimate for this data element, which was originally NULL"

  def init(classifier: Classifier, classIndex: Int) = {
    cIndex = classIndex
    learner = classifier
  }

  def init(source: Operator, classIndex: Int) = {

    /* Learn the model */
    cIndex = classIndex
    // println("SOURCE: "+source)
    val iterator = lens.db.query(source)
    val attributes = getAttributesFromIterator(iterator)
    data = new Instances("TrainData", attributes, 100)

    var numInstances = 0
    iterator.open()

    /* The second check poses a limit on the learning data and reduces time spent building the lens */
    while(iterator.getNext() && numInstances < TRAINING_LIMIT) {
      // println("ROW: "+iterator.currentRow())
      val instance = new DenseInstance(iterator.numCols)
      instance.setDataset(data)
      for(j <- 0 until iterator.numCols                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       ) {
        iterator.schema(j)._2 match {
          case TInt() | TFloat() =>
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
    data.setClassIndex(classIndex)

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

  def getLearner = learner

  def classify(rowid: RowIdPrimitive): List[(Double, Int)] = {
    //println("Classify: "+rowid)
    val rowValues = lens.db.query(
        Select(
          Comparison(Cmp.Eq, RowIdVar(), rowid),
          lens.source
        )
    )
    rowValues.open()
    if (!rowValues.getNext()) {
      throw new SQLException("Invalid Source Data ROWID: " + rowid);
    }
    val row = new DenseInstance(rowValues.numCols)
    val attributes = getAttributesFromIterator(rowValues)
    data = new Instances("TestData", attributes, 1)
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

  private def getAttributesFromIterator(iterator: ResultIterator): util.ArrayList[Attribute] = {
    val attributes = new util.ArrayList[Attribute]()
    iterator.schema.foreach { case (n, t) =>
      (n, t) match {
        case (_, TRowId()) => attributes.add(new Attribute(n, null.asInstanceOf[util.ArrayList[String]]))
        case (_, TInt() | TFloat()) => attributes.add(new Attribute(n))
        case _ => attributes.add(new Attribute(n, null.asInstanceOf[util.ArrayList[String]]))
      }
    }

    attributes
  }

  private def computeMostLikelyValue(args: List[PrimitiveValue]): PrimitiveValue = {
    val att = learner.getModelContext.attribute(cIndex)
    val classes = classify(args(0).asInstanceOf[RowIdPrimitive])
    val res = if (classes.isEmpty) att.numValues() - 1 else classes.maxBy(_._1)._2
    if (att.isString)
      StringPrimitive(att.value(res))
    else if (att.isNumeric)
      IntPrimitive(res)
    else
      throw new SQLException("Unknown type")
  }

  ////// Model implementation
  def varType(argTypes: List[Type]): Type = varType

  def bestGuess(args: List[PrimitiveValue]): PrimitiveValue = {
    val att = learner.getModelContext.attribute(cIndex)
    val classes = classify(args(0).asInstanceOf[RowIdPrimitive])
    val res = if (classes.isEmpty) att.numValues() - 1 else classes.maxBy(_._1)._2
    if (att.isString)
      StringPrimitive(att.value(res))
    else if (att.isNumeric)
      IntPrimitive(res)
    else
      throw new SQLException("Unknown type")

//    val res = lens.db.query(
//      "SELECT DATA, TYPE FROM __"+name+"_BACKEND WHERE EXP_LIST = "+args.map(x => x.asString).mkString("|")+";"
//    )
//    if(!res.getNext()) throw new SQLException("Value not found for "+name+". Explist: "+args.mkString("|"))
//    res(1).asString match {
//      case "TInt()" => IntPrimitive(res(0).asLong)
//      case "TFloat()" => FloatPrimitive(res(0).asDouble)
//      case _ => res(0)
//    }
  }

  def lowerBound(args: List[PrimitiveValue]) = {
    val att = learner.getModelContext.attribute(cIndex)
    val classes = classify(args(0).asInstanceOf[RowIdPrimitive])
    val res = if (classes.isEmpty) att.numValues() - 1 else classes.minBy(_._2)._2
    if (att.isString)
      StringPrimitive(att.value(res))
    else if (att.isNumeric)
      IntPrimitive(res)
    else
      throw new SQLException("Unknown type")
  }

  def upperBound(args: List[PrimitiveValue]) = {
    val att = learner.getModelContext.attribute(cIndex)
    val classes = classify(args(0).asInstanceOf[RowIdPrimitive])
    val res = if (classes.isEmpty) att.numValues() - 1 else classes.maxBy(_._2)._2
    if (att.isString)
      StringPrimitive(att.value(res))
    else if (att.isNumeric)
      IntPrimitive(res)
    else
      throw new SQLException("Unknown attribute")
  }

  def sample(randomness: Random, args: List[PrimitiveValue]): PrimitiveValue = {
    val classes = classify(args(0).asInstanceOf[RowIdPrimitive])
    if(classes.length < 1){ return NullPrimitive(); }
    val tot_cnt = classes.map(_._1).sum
    val pick = randomness.nextInt(100) % tot_cnt
    val cumulative_counts =
      classes.scanLeft(0.0)(
        (cumulative, cnt_class) => cumulative + cnt_class._1
      )
    val pick_idx: Int = cumulative_counts.indexWhere(pick < _) - 1
    IntPrimitive(classes(pick_idx)._2)
  }
}
