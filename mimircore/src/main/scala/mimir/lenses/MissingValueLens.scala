package mimir.lenses;

import java.sql._

import mimir.{Analysis, Database}
import mimir.algebra._
import mimir.ctables._
import moa.classifiers.Classifier
import moa.core.InstancesHeader
import weka.core.{DenseInstance, Instance, Instances}
import weka.experiment.{DatabaseUtils, InstanceQuery, InstanceQueryAdapter}

import scala.collection.JavaConversions._
import scala.util._;

class MissingValueLens(name: String, args: List[Expression], source: Operator) 
  extends Lens(name, args, source) 
  with InstanceQueryAdapter
{
  var orderedSourceSchema: List[(String,Type.T)] = null
  val keysToBeCleaned = args.map( Eval.evalString(_).toUpperCase )
  var models: List[SingleVarModel] = null;
  var data: Instances = null
  var model: Model = null
  var db: Database = null

  def sourceSchema() = {
    if(orderedSourceSchema == null){
      orderedSourceSchema = 
        source.schema.toList.map( _ match { case (n,t) => (n.toUpperCase,t) } )
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
    val schema = source.schema.keys.map(_.toUpperCase).toList;
    val results = db.backend.execute(db.convert(source));
    models =
      sourceSchema.map(
          _ match { case (n,t) => (keysToBeCleaned.indexOf(n), t) }
        ).map( _ match { case (idx,t) => 
          if(idx < 0){ 
            new NoOpModel(t).asInstanceOf[SingleVarModel]
          } else {
            val m = new MissingValueModel(this);
            data = InstanceQuery.retrieveInstances(this, results);
            val classIndex = idx//allKeys().indexOf(keysToBeCleaned.get(idx))
            data.setClassIndex(classIndex);
            m.init(data);
            m.asInstanceOf[SingleVarModel];
          }
        })
    model = new JointSingleVarModel(models);
  }
  
  def lensType = "MISSING_VALUE"

  ////// Weka's InstanceQueryAdapter interface
  def attributeCaseFix(colName: String) = colName;
  def getDebug() = false;
  def getSparseData() = false;
  def translateDBColumnType(t: String) = {
    t.toUpperCase() match { 
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
  val MOST_LIKELY, LOWER_BOUND, UPPER_BOUND, VARIANCE, CONFIDENCE = Value
}

case class MissingValueAnalysis(model: MissingValueModel, args: List[Expression], analysisType: MissingValueAnalysisType.MV)
  extends Proc(args) {
  def get(args: List[PrimitiveValue]): PrimitiveValue = {
    analysisType match {
      case MissingValueAnalysisType.VARIANCE => model.variance(args)
      case MissingValueAnalysisType.CONFIDENCE => model.confidenceInterval(args)
      case MissingValueAnalysisType.LOWER_BOUND => model.lowerBound(args)
      case MissingValueAnalysisType.UPPER_BOUND => model.upperBound(args)
      case MissingValueAnalysisType.MOST_LIKELY => model.mostLikelyValue(args)
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

  def init(d: Instances) = {
    data = d
    learner.setModelContext(new InstancesHeader(data));
    learner.prepareForUse();
    data.foreach( learn(_) );
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
    val row = new DenseInstance(lens.allKeys.length);
    (0 until lens.allKeys.length).foreach( (col) => {
      val v = rowValues(col)
      if(!v.isInstanceOf[NullPrimitive]){
        row.setValue(col, v.asDouble)
      }
    })
    row.setDataset(data)
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
  def variance(args: List[PrimitiveValue]) = {
    val classes = classify(args(0))
    val totSum = classes.foldRight(0.0){(a, b) => b + a._1}
    val mean = classes.foldRight(0.0){(a, b) => b + (a._1 * a._2 / totSum)}
    val variance = classes.foldRight(0.0){
      (a, b) => b + ((a._2) - mean) * ((a._2) - mean) * a._1/totSum
    } / classes.size
    FloatPrimitive(variance)
  }
  def confidenceInterval(args: List[PrimitiveValue]) = {
    var samples = List[Double]()
    var seed = 1
    var sum = 0.0
    var variance = 0.0
    for( i <- 0 until 100) {
      val sample = this.sample(seed, args).asDouble
      sum += sample
      samples ::= sample
      seed += 1
    }
    val mean = sum/samples.size
    for(i <- samples.indices){
      variance += (samples(i) - mean) * (samples(i) - mean)
    }
    val conf = Math.sqrt(variance/samples.size-1) * 1.96
    //TODO check this for 95% confidence level
    FloatPrimitive(conf)
  }
  def mostLikelyExpr(args: List[Expression]) = {
    new MissingValueAnalysis(this, args, MissingValueAnalysisType.MOST_LIKELY)
  }
  def lowerBoundExpr(args: List[Expression]) = {
      new MissingValueAnalysis(this, args, MissingValueAnalysisType.LOWER_BOUND)
  }
  def upperBoundExpr(args: List[Expression]) = {
    new MissingValueAnalysis(this, args, MissingValueAnalysisType.UPPER_BOUND)
  }
  def varianceExpr(args: List[Expression]) = {
    new MissingValueAnalysis(this, args, MissingValueAnalysisType.VARIANCE)
  }
  def confidenceExpr(args: List[Expression]) = {
    new MissingValueAnalysis(this, args, MissingValueAnalysisType.CONFIDENCE)
  }
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
// class MissingValueAnalysis(db: Database, idx: Int, ctx: MissingValueLens) extends CTAnalysis(db) {
//   def varType: Type.T = Type.TInt
//   def isCategorical: Boolean = true
  

  
//   def computeMLE(element: List[PrimitiveValue]): PrimitiveValue = 
//   {
//     val classes = classify(element(0));
//     var maxClass = 0;
//     var maxLikelihood = classes(0);
//     (1 until classes.length).foreach( (i) => {
//       if(classes(i) > maxLikelihood){ 
//         maxLikelihood = classes(i);
//         maxClass = i
//       }
//     })
//     return new IntPrimitive(maxClass)
//   }
//   def computeEqConfidence(element: List[PrimitiveValue], value: PrimitiveValue): Double = 0.0
//   def computeBounds(element: List[PrimitiveValue]): (Double,Double) = (0.0,0.0)
//   def computeStdDev(element: List[PrimitiveValue]): Double = 0.0
  
// }