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
import mimir.Analysis.Model;
import mimir.sql.Backend;
import mimir.ctables._;
import mimir.algebra._;
import mimir.util._;

class MissingValueModule(name: String, id: Int, params: List[String]) 
  extends IViewModule(name, id, params) 
  with InstanceQueryAdapter
{

  var models: List[Model] = null;

  def wrap(source: Operator): Operator = {
    val keysToBeCleaned = params.map( _.toUpperCase )
    val allKeys = source.schema.keys
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
  def build(backend: Backend, source: Operator): Unit = {
    val schema = source.schema.keys.map(_.toUpperCase).toList;
    val results = backend.execute(source);
    models = params.map( (v) => {
      val learner: Classifier = 
        Analysis.getLearner("moa.classifiers.bayes.NaiveBayes");
      val idx = schema.indexOf(v.toUpperCase) 
      if(idx < 0){ 
        throw new SQLException("Invalid attribute: '"+v+"' in "+schema.toString());
      }
      println("Building learner for '"+v+"'");
      val data = 
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
      new Model(learner, data, numSamples, numCorrect);
    })
    // Now save the models to the backend.
    
  }
  
  def load(backend: Backend): Unit = {
    
  }
  
  def varCount(): Int = params.length;
  def moduleType = "MISSING_VALUE"
  
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