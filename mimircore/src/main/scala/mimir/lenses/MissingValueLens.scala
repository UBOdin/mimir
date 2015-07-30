package mimir.lenses

import java.sql._
import java.util

import mimir.algebra._
import mimir.ctables._
import mimir.exec.ResultIterator
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
  with InstanceQueryAdapter
{
  var orderedSourceSchema: List[(String,Type.T)] = null
  val keysToBeCleaned = args.map( Eval.evalString(_).toUpperCase )
  var models: List[SingleVarModel] = null
  var model: Model = null
  var db: Database = null

  def sourceSchema() = {
    if(orderedSourceSchema == null){
      orderedSourceSchema = 
        source.schema.map( _ match { case (n,t) => (n.toUpperCase,t) } )
    }
    orderedSourceSchema
  }

  def allKeys() = { sourceSchema.map(_._1) }

  def view: Operator = {
    Project(
      allKeys().
        map( (k) => {
          val v = keysToBeCleaned.indexOf(k);
          if(v >= 0){
            val u = allKeys().indexOf(k)
            ProjectArg(k,
              CaseExpression(
                List(WhenThenClause(
                  mimir.algebra.IsNullExpression(Var(k), false),
                  rowVar(u)
                )),
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
    this.db = db
    val iterator = db.query(source)
    models =
      sourceSchema.map(
          _ match { case (n,t) => (keysToBeCleaned.indexOf(n), t) }
        ).map( _ match { case (idx,t) => 
          if(idx < 0){ 
            new NoOpModel(t).asInstanceOf[SingleVarModel]
          } else {
            val m = new MissingValueModel(this)
            val classIndex = idx//allKeys().indexOf(keysToBeCleaned.get(idx))
            m.init(iterator, classIndex)
            m.asInstanceOf[SingleVarModel]
          }
        })
    model = new JointSingleVarModel(models)
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
  def exprType(bindings: Map[String,Type.T]) = Type.TInt
  def rebuild(c: List[Expression]) = new MissingValueAnalysis(model, c, analysisType)
}

class MissingValueModel(lens: MissingValueLens)
  extends SingleVarModel(Type.TInt) 
{
  val learner: Classifier = 
        Analysis.getLearner("moa.classifiers.bayes.NaiveBayes");
  var data: Instances = null; 
  var numCorrect = 0;
  var numSamples = 0;
  def reason(): String =
    "Missing values for attribute(s) "+lens.keysToBeCleaned.mkString(", ")

  def init(iterator: ResultIterator, classIndex: Int) = {
    val attInfo = new util.ArrayList[Attribute]()
    iterator.schema.foreach{ case (n, t) =>
      t match {
        case Type.TInt | Type.TFloat => attInfo.add(new Attribute(n))
        case _ => attInfo.add(new Attribute(n, null.asInstanceOf[util.ArrayList[String]]))
      }
    }
    val rawData = iterator.allRows()
    data = new Instances("Dataset", attInfo, rawData.length)

    rawData.indices.foreach( (i) => {
      val instance = new DenseInstance(rawData.head.length)
      instance.setDataset(data)
      rawData(i).indices.foreach((j) => {
        iterator.schema(j)._2 match {
          case Type.TInt | Type.TFloat => {
            try {
              instance.setValue(j, rawData(i)(j).asDouble)
            } catch {
              case e: TypeException =>
            }
          }
          case _ => instance.setValue(j, rawData(i)(j).asString)
        }
      })
      data.add(instance)
    })
    data.setClassIndex(classIndex)

    learner.setModelContext(new InstancesHeader(data))
    learner.prepareForUse()
    data.foreach( learn(_) )
  }

  def learn(dataPoint: Instance) = {
    numSamples += 1;
    if(learner.correctlyClassifies(dataPoint)){
      numCorrect += 1;
    }
    learner.trainOnInstance(dataPoint);
  }

  def classify(rowid: PrimitiveValue): List[(Double, Int)] =
  {
    val rowValues = lens.db.query(
      CTPercolator.percolate(
        Select(
          Comparison(Cmp.Eq, Var("ROWID"), rowid),
          lens.source
        )
      )
    )
    if(!rowValues.getNext()){
      throw new SQLException("Invalid Source Data ROWID: '" +rowid+"'");
    }
    val row = new DenseInstance(lens.allKeys.length)
    row.setDataset(data)
    (0 until lens.allKeys.length).foreach( (col) => {
      val v = rowValues(col+1)
      if(!v.isInstanceOf[NullPrimitive]){
        if(v.isInstanceOf[IntPrimitive] || v.isInstanceOf[FloatPrimitive]) {
          row.setValue(col, v.asDouble)
        }
        else {
          row.setValue(col, v.asString)
        }
      }
    })
    learner.getVotesForInstance(row).
      toList.
      zipWithIndex.
      filter( _._1 > 0 );
  }

  ////// Model implementation
  def mostLikelyValue(args: List[PrimitiveValue]): PrimitiveValue = {
    IntPrimitive(classify(args(0)).maxBy(_._1)._2);
  }
  def lowerBound(args: List[PrimitiveValue]) = {
      val classes = classify(args(0));
      IntPrimitive(classes.minBy(_._2)._2)
  }
  def upperBound(args: List[PrimitiveValue]) = {
      val classes = classify(args(0));
      IntPrimitive(classes.maxBy(_._2)._2)
  }
  def sampleGenerator(args: List[PrimitiveValue]) = {
    var hash = 7
    val key = lens.keysToBeCleaned(0)
    for(i <- key.indices){
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
  def sample(seed: Long, args: List[PrimitiveValue]):  PrimitiveValue = {
      val classes = classify(args(0))
      val tot_cnt = classes.map(_._1).sum
      val pick = new Random(seed).nextInt(100) % tot_cnt
      val cumulative_counts = 
        classes.scanLeft(0.0)(
            ( cumulative, cnt_class ) => cumulative + cnt_class._1 
        )
      val pick_idx: Int = cumulative_counts.indexWhere( pick < _ ) - 1
      IntPrimitive(classes(pick_idx)._2)
  }
}