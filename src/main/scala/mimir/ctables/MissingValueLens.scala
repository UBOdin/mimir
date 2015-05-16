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
{
  var orderedSourceSchema: List[(String,Type.T)] = null
  val keysToBeCleaned = params.map( _.toUpperCase )
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
            data = 
              InstanceQuery.retrieveInstances(this, results);
            new MissingValueModel(this, data).asInstanceOf[SingleVarModel]
          }
        })
    model = new JointSingleVarModel(models);
  }
  
  def save(db: Database): Unit = { /* Ignore for now */ }

  def load(db: Database): Unit = { build(db); }
  
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

class MissingValueModel(lens: MissingValueLens)
  extends SingleVarModel(Type.TInt) 
  with InstanceQueryAdapter
{
  val learner: Classifier = 
        Analysis.getLearner("moa.classifiers.bayes.NaiveBayes");
  var numCorrect = 0;
  var numSamples = 0;

  def varType: Type.T = Type.TInt

  def init(ctx: InstancesHeader) = {
    learner.setModelContext(ctx);
    learner.prepareForUse();
  }

  def buildClassifier()
            data.setClassIndex(idx);
            m.init(data);
            new InstancesHeader(data));

            data.foreach( m.learn(_) )

  def learn(dataPoint: Instance) = {
    numSamples += 1;
    if(learner.correctlyClassifies(dataPoint)){
      numCorrect += 1;
    }
    learner.trainOnInstance(dataPoint);
  }

  def classify(rowid: PrimitiveValue) =
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
    val row = new DenseInstance(ctx.allKeys.length);
    (0 until lens.allKeys.length).foreach( (col) => {
      val v = rowValues(col)
      if(!v.isInstanceOf[NullPrimitive]){
        row.setValue(col, v.asDouble)
      }
    })
    row.setDataset(ctx.data)
    learner.getVotesForInstance(row)
  }

  ////// Model implementation
  def mostLikelyValue(args: List[PrimitiveValue]): PrimitiveValue =
    { classify(args(0)); }
  def boundsValues(args: List[PrimitiveValue]): (PrimitiveValue, PrimitiveValue)
  def boundsExpressions(args: List[Expression    ]): (Expression, Expression)
  def sample(seed: Long, args: List[PrimitiveValue]):  PrimitiveValue

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