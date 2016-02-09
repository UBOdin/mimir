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

import scala.collection.JavaConversions._
import scala.util._
;

class MissingValueLens(name: String, args: List[Expression], source: Operator)
  extends Lens(name, args, source)
  with InstanceQueryAdapter {
  var orderedSourceSchema: List[(String, Type.T)] = null
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

  def schema(): List[(String, Type.T)] = sourceSchema()

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
      sourceSchema.map(
        _ match { case (n, t) => (keysToBeCleaned.indexOf(n), t) }
      ).zipWithIndex.map( (x) => x._1 match { case (idx, t) =>
        if (idx < 0) {
          new NoOpModel(t).asInstanceOf[SingleVarModel]
        } else {
          val m = new MissingValueModel(this, name+"_"+x._2)
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
    model = new JointSingleVarModel(models)
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

object MissingValueAnalysisType extends Enumeration {
  type MV = Value
  val MOST_LIKELY, LOWER_BOUND, UPPER_BOUND, SAMPLE = Value
}

case class MissingValueAnalysis(model: MissingValueModel, args: List[Expression], analysisType: MissingValueAnalysisType.MV)
  extends Proc(args) {
  def get(args: List[PrimitiveValue]): PrimitiveValue = {
    analysisType match {
      case MissingValueAnalysisType.LOWER_BOUND => model.lowerBound(args)
      case MissingValueAnalysisType.UPPER_BOUND => model.upperBound(args)
      case MissingValueAnalysisType.MOST_LIKELY => model.mostLikelyValue(args)
      case MissingValueAnalysisType.SAMPLE => model.sampleGenerator(args)
    }
  }

  def getType(bindings: List[Type.T]) = {
    val att = model.getLearner.getModelContext().attribute(model.cIndex)
    if (att.isString)
      Type.TString
    else if (att.isNumeric)
      Type.TFloat
    else
      throw new SQLException("Unknown type")
  }

  def rebuild(c: List[Expression]) = new MissingValueAnalysis(model, c, analysisType)
}

class MissingValueModel(lens: MissingValueLens, name: String)
  extends SingleVarModel(Type.TInt) {
  var learner: Classifier =
    Analysis.getLearner("moa.classifiers.bayes.NaiveBayes");
  var data: Instances = null;
  var numCorrect = 0;
  var numSamples = 0;
  var cIndex = 0;
  def backingStore() = name+"_BACKEND"

  def reason(args: List[Expression]): (String, String) =
    ("I made a best guess estimate for this data element, which was originally NULL", "MISSING_VALUE")

  def init(classifier: Classifier, classIndex: Int) = {
    cIndex = classIndex
    learner = classifier
  }

  def init(source: Operator, classIndex: Int) = {
    cIndex = classIndex
    // println("SOURCE: "+source)
    val iterator = lens.db.query(source)
    val attributes = getAttributesFromIterator(iterator)
    data = new Instances("TrainData", attributes, 100)

    var numInstances = 0
    iterator.open()
    while(iterator.getNext() && numInstances < 10000) {
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
    data.setClassIndex(classIndex)

    learner.setModelContext(new InstancesHeader(data))
    learner.prepareForUse()
    data.foreach(learn(_))

    if(lens.db.getTableSchema(backingStore()).isEmpty) {
      lens.db.update(
        "CREATE TABLE "+backingStore()+""" (
                                         | EXP_LIST varchar(100),
                                         | DATA varchar(100),
                                         | TYPE char(10),
                                         | ACCEPTED char(1),
                                         | PRIMARY KEY (EXP_LIST)
                                         | )""".stripMargin
      )
    }
    else {
      lens.db.update( "DELETE FROM "+backingStore() )
    }

    val rowidIterator = lens.db.query(source)

    rowidIterator.open()
    while(rowidIterator.getNext()) {
      if(Typechecker.typeOf(rowidIterator(classIndex+1)) == Type.TAny) {
        val explist = List(rowidIterator.provenanceToken())
        val data = computeMostLikelyValue(explist)
        val typ = TypeUtils.convert(data.getType)
        val accepted = "N"
        val tuple = List(explist.map(x => x.asString).mkString("|"), data.asString, typ, accepted)
        lens.db.update(
          "INSERT INTO "+backingStore()+" VALUES (?, ?, ?, ?)", tuple
        )
      }
    }
    rowidIterator.close()
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
    // println("Classify: "+rowid)
    val rowValues = lens.db.query(
      CTPercolator.percolate(
        Select(
          Comparison(Cmp.Eq, Var(CTPercolator.ROWID_KEY), rowid),
          lens.source
        )
      )
    )
    rowValues.open()
    if (!rowValues.getNext()) {
      throw new SQLException("Invalid Source Data ROWID: '" + rowid + "'");
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
        case ("ROWID_MIMIR", _) => attributes.add(new Attribute(n, null.asInstanceOf[util.ArrayList[String]]))
        case (_, Type.TInt | Type.TFloat) => attributes.add(new Attribute(n))
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
  def mostLikelyValue(args: List[PrimitiveValue]): PrimitiveValue = {
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
//      case "TInt" => IntPrimitive(res(0).asLong)
//      case "TFloat" => FloatPrimitive(res(0).asDouble)
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

  def sampleGenerator(args: List[PrimitiveValue]) = {
    var hash = 7
    val key = lens.keysToBeCleaned(0)
    for (i <- key.indices) {
      hash = hash * 31 + key.charAt(i)
    }
    val seed = {
      if (args.length == 1)
        java.lang.System.nanoTime()
      else args.last.asLong
    } + hash
    sample(seed, args)
  }

  def mostLikelyExpr(args: List[Expression]) =
    new MissingValueAnalysis(this, args, MissingValueAnalysisType.MOST_LIKELY)

  def lowerBoundExpr(args: List[Expression]) =
    new MissingValueAnalysis(this, args, MissingValueAnalysisType.LOWER_BOUND)

  def upperBoundExpr(args: List[Expression]) =
    new MissingValueAnalysis(this, args, MissingValueAnalysisType.UPPER_BOUND)

  def sampleGenExpr(args: List[Expression]) =
    new MissingValueAnalysis(this, args, MissingValueAnalysisType.SAMPLE)

  def sample(seed: Long, args: List[PrimitiveValue]): PrimitiveValue = {
    val classes = classify(args(0).asInstanceOf[RowIdPrimitive])
    val tot_cnt = classes.map(_._1).sum
    val pick = new Random(seed).nextInt(100) % tot_cnt
    val cumulative_counts =
      classes.scanLeft(0.0)(
        (cumulative, cnt_class) => cumulative + cnt_class._1
      )
    val pick_idx: Int = cumulative_counts.indexWhere(pick < _) - 1
    IntPrimitive(classes(pick_idx)._2)
  }
}
