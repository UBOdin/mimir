package mimir.ctables;

import java.sql._;
import collection.JavaConversions._;

import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;
import weka.experiment.DatabaseUtils;
import weka.experiment.InstanceQuery;
import weka.experiment.InstanceQueryAdapter;
import moa.classifiers.Classifier;
import moa.core.InstancesHeader;
import moa.streams.ArffFileStream;
import moa.streams.InstanceStream;

import mimir.Analysis;
import mimir.{Database,Mimir};
import mimir.algebra._;
import mimir.util._;
import mimir.exec._;

class MissingValueLens(name: String, params: List[String], source: Operator) 
  extends Lens(name, params, source) 
  with InstanceQueryAdapter
{
  var allKeys: List[String] = null
  var models: List[Analysis.Model] = null;
  var data: Instances = null
  var model: Model = null

  def view: Operator = {
    val keysToBeCleaned = params.map( _.toUpperCase )
    allKeys = source.schema.keys.toList
    Project(
      allKeys.
        map( (k) => {
          val v = keysToBeCleaned.indexOf(k);
          if(v >= 0){
            ProjectArg(k,
              CaseExpression(
                List(WhenThenClause(
                  mimir.algebra.IsNullExpression(Var(k), false),
                  rowVar(v)
                )),
                Var(k)
              ))
          } else {
            ProjectArg(k, Var(k))
          }
        }).toList,
      source
    )
  }
  def build(db: Database): Unit = {
    val schema = source.schema.keys.map(_.toUpperCase).toList;
    val results = db.backend.execute(db.convert(source));
    models = params.map( (v) => {
      val learner: Classifier = 
        Analysis.getLearner("moa.classifiers.bayes.NaiveBayes");
      val idx = schema.indexOf(v.toUpperCase) 
      if(idx < 0){ 
        throw new SQLException("Invalid attribute: '"+v+"' in "+schema.toString());
      }
      if(!Mimir.conf.quiet()){
        println("Building learner for '"+v+"'");
      }
      data = 
        InstanceQuery.retrieveInstances(this, results);
      data.setClassIndex(idx);
      learner.setModelContext(new InstancesHeader(data));
      learner.prepareForUse();
      var numCorrect: Int = 0;
      var numSamples: Int = 0;
      data.map( (dataPoint) => {
        numSamples += 1;
        if(learner.correctlyClassifies(dataPoint)){
          numCorrect += 1;
        }
        learner.trainOnInstance(dataPoint);
      })
      new Analysis.Model(learner, data, numSamples, numCorrect);
    })
    // Now save the models to the backend.
    
  }
  
  def load(db: Database): Unit = 
  {
    
  }
  
  def varCount: Int = params.length;
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

// class MissingValueAnalysis(db: Database, idx: Int, ctx: MissingValueLens) extends CTAnalysis(db) {
//   def varType: Type.T = Type.TInt
//   def isCategorical: Boolean = true
  
//   def classify(rowid: PrimitiveValue) =
//   {
//     val rowValues = db.query(
//       CTPercolator.percolate(
//         Select(
//           Comparison(Cmp.Eq, Var("ROWID"), rowid),
//           ctx.source
//         )
//       )
//     )
//     if(!rowValues.getNext()){
//       throw new SQLException("Invalid Source Data ROWID: '" +rowid+"'");
//     }
//     val row = new DenseInstance(ctx.allKeys.length);
//     (0 until ctx.allKeys.length).foreach( (col) => {
//       val v = rowValues(col)
//       if(!v.isInstanceOf[NullPrimitive]){
//         row.setValue(col, v.asDouble)
//       }
//     })
//     row.setDataset(ctx.data)
//     ctx.models(idx).classifier.getVotesForInstance(row)
//   }
  
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