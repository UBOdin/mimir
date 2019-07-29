package mimir.ml.spark

import mimir.algebra._
import mimir.Database

import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, BooleanType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor, GBTRegressor, DecisionTreeRegressor, LinearRegression, GeneralizedLinearRegression, IsotonicRegression}
import mimir.util.SparkUtils
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import mimir.exec.spark.RAToSpark
import mimir.provenance.Provenance


object Regression extends SparkML {
  
  def regressDB(model : PipelineModel, query : Operator, db:Database) : DataFrame = {
    applyModelDB(model, query, db)
  }
  
  def regress( model : PipelineModel, cols:Seq[(ID, Type)], testData : List[Seq[PrimitiveValue]], db: Database): DataFrame = {
    applyModel(model, cols, testData, db)
  }
  
  override def extractPredictions(model : PipelineModel, predictions:DataFrame, maxPredictions:Int = 5) : Seq[(String, (String, Double))] = {
    val sqlContext = getSparkSqlContext()
    import sqlContext.implicits._  
    predictions.select(
      Provenance.rowidColnameBase.id,
      "prediction"
    ).rdd.map { r => r.getString(0) -> r.getDouble(1) }
         .collect()
         .map{ item => item._1 -> (item._2.toString(), 1.0) }
         .toSeq
  }
  
  override def extractPredictionsForRow(model : PipelineModel, predictions:DataFrame, rowid:String, maxPredictions:Int = 5) : Seq[(String, Double)] = {
    val sqlContext = getSparkSqlContext()
    import sqlContext.implicits._  
    predictions
      .where( predictions(Provenance.rowidColnameBase.id) === rowid )
      .select("prediction")
      .rdd
      .map(r => r.getDouble(1))
      .collect()
      .map { item => item.toString() -> 1.0 }
      .toSeq    
  }
  
  def RandomForestRegressorModel(trainingData:DataFrame):SparkML.SparkModelGenerator = params => {
    val training = trainingData
    val cols = training.schema.fields.tail
    val assmblerCols = cols.flatMap(col => {
      col.dataType match {
        case IntegerType | LongType | DoubleType => Some(col.name)
        case _ => None
      }
    })
    val assembler = new VectorAssembler().setInputCols(assmblerCols.toArray).setOutputCol("features")
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(2)
      
    
    // Train a RandomForest model.
    val rf = new RandomForestRegressor()
      .setLabelCol(params.predictionCol.id)
      .setFeaturesCol("indexedFeatures")
     
    // Chain indexer and forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(assembler, featureIndexer, rf))
      
    pipeline.fit(training)
  }
  
  def GradientBoostedTreeRegressorModel(trainingData:DataFrame):SparkML.SparkModelGenerator = params => {
    val training = trainingData.transform(fillNullValues)
    val cols = training.schema.fields.tail
    val assmblerCols = cols.flatMap(col => {
      col.dataType match {
        case IntegerType | LongType | DoubleType => Some(col.name)
        case _ => None
      }
    })
    val assembler = new VectorAssembler().setInputCols(assmblerCols.toArray).setOutputCol("features")
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
    
    // Train a  model.
    val rf = new GBTRegressor()
      .setLabelCol(params.predictionCol.id)
      .setFeaturesCol("indexedFeatures")
      .setMaxIter(10)
      
    // Chain indexer and forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(assembler, featureIndexer, rf))
     
    pipeline.fit(training)
  }
  
  
  def DecisionTreeRegressorModel(trainingData:DataFrame):SparkML.SparkModelGenerator = params => {
    val training = trainingData.transform(fillNullValues)
    val cols = training.schema.fields.tail
    val assmblerCols = cols.flatMap(col => {
      col.dataType match {
        case IntegerType | LongType | DoubleType => Some(col.name)
        case _ => None
      }
    })
    val assembler = new VectorAssembler().setInputCols(assmblerCols.toArray).setOutputCol("features")
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
    
    // Train a  model.
    val rf = new DecisionTreeRegressor()
      .setLabelCol(params.predictionCol.id)
      .setFeaturesCol("indexedFeatures")
      
    // Chain indexer and forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(assembler, featureIndexer, rf))
      
    pipeline.fit(training)
  }
  
  def LinearRegressorModel(trainingData:DataFrame):SparkML.SparkModelGenerator = params => {
    val training = trainingData.transform(fillNullValues)
    val cols = training.schema.fields.tail
    val assmblerCols = cols.flatMap(col => {
      col.dataType match {
        case IntegerType | LongType | DoubleType => Some(col.name)
        case _ => None
      }
    })
    val assembler = new VectorAssembler().setInputCols(assmblerCols.toArray).setOutputCol("features")
    
    // Train a  model.
    val rf = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setLabelCol(params.predictionCol.id)
      .setFeaturesCol("features")
      
    // Chain indexer and forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(assembler, rf))
     
    pipeline.fit(training)
  }
  
  def GeneralizedLinearRegressorModel(trainingData:DataFrame):SparkML.SparkModelGenerator = params => {
    val training = trainingData.transform(fillNullValues)
    val cols = training.schema.fields.tail
    val assmblerCols = cols.flatMap(col => {
      col.dataType match {
        case IntegerType | LongType | DoubleType => Some(col.name)
        case _ => None
      }
    })
    val assembler = new VectorAssembler().setInputCols(assmblerCols.toArray).setOutputCol("features")
   
    // Train a  model.
    val rf = new GeneralizedLinearRegression()
      .setFamily("gaussian")
      .setLink("identity")
      .setMaxIter(10)
      .setRegParam(0.3)
      .setLabelCol(params.predictionCol.id)
      .setFeaturesCol("features")
      
    // Chain indexer and forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(assembler, rf))
   
    pipeline.fit(training)
  }
  
  def IsotonicRegressorModel(trainingData:DataFrame):SparkML.SparkModelGenerator = params => {
    val training = trainingData.transform(fillNullValues)
    val cols = training.schema.fields.tail
    val assmblerCols = cols.flatMap(col => {
      col.dataType match {
        case IntegerType | LongType | DoubleType => Some(col.name)
        case _ => None
      }
    })
    val assembler = new VectorAssembler().setInputCols(assmblerCols.toArray).setOutputCol("features")
   
    // Train a  model.
    val rf = new IsotonicRegression().setLabelCol(params.predictionCol.id).setFeaturesCol("features")
      
    // Chain indexer and forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(assembler, rf))
   
    pipeline.fit(training)
  }
}