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
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor, GBTRegressor, DecisionTreeRegressor, LinearRegression, GeneralizedLinearRegression}


object Regression extends SparkML {
  
  def regressDB(model : PipelineModel, query : Operator, db:Database, valuePreparer:ValuePreparer = prepareValueApply, sparkTyper:Type => DataType = getSparkType) : DataFrame = {
    applyModelDB(model, query, db, valuePreparer, sparkTyper)
  }
  
  def regress( model : PipelineModel, cols:Seq[(String, Type)], testData : List[Seq[PrimitiveValue]], valuePreparer:ValuePreparer = prepareValueApply, sparkTyper:Type => DataType = getSparkType): DataFrame = {
    applyModel(model, cols, testData, valuePreparer, sparkTyper)
  }
  
  override def prepareValueTrain(value:PrimitiveValue, t:Type): Any = {
    value match {
      case NullPrimitive() => null
      case RowIdPrimitive(s) => s
      case StringPrimitive(s) => s
      case IntPrimitive(i) => i
      case FloatPrimitive(f) => f
      case x =>  x.asString 
    }
  }
  
  override def prepareValueApply(value:PrimitiveValue, t:Type): Any = {
    value match {
      case NullPrimitive() => t match {
        case TInt() => 0L
        case TFloat() => 0.0
        case TDate() => ""
        case TString() => ""
        case TBool() => false
        case TRowId() => ""
        case TType() => ""
        case TAny() => ""
        case TTimestamp() => ""
        case TInterval() => ""
        case TUser(name) => prepareValueApply(value, mimir.algebra.TypeRegistry.registeredTypes(name)._2)
        case x => ""
      }
      case RowIdPrimitive(s) => s
      case StringPrimitive(s) => s
      case IntPrimitive(i) => i
      case FloatPrimitive(f) => f
      case x =>  x.asString 
    }
  }
  
  override def getSparkType(t:Type) : DataType = {
    t match {
      case TInt() => LongType
      case TFloat() => DoubleType
      case TDate() => StringType
      case TString() => StringType
      case TBool() => BooleanType
      case TRowId() => StringType
      case TType() => StringType
      case TAny() => StringType
      case TTimestamp() => StringType
      case TInterval() => StringType
      case TUser(name) => getSparkType(mimir.algebra.TypeRegistry.registeredTypes(name)._2)
      case _ => StringType
    }
  }
  
  override def extractPredictions(model : PipelineModel, predictions:DataFrame, maxPredictions:Int = 5) : Seq[(String, (String, Double))] = {
    val sqlContext = getSparkSqlContext()
    import sqlContext.implicits._  
    predictions.select("rowid","prediction").rdd.map(r => (r.getString(0), r.getDouble(1))).collect().map{ item =>
        (item._1, (item._2.toString(), 1.0))}.toSeq
  }
  
  override def extractPredictionsForRow(model : PipelineModel, predictions:DataFrame, rowid:String, maxPredictions:Int = 5) : Seq[(String, Double)] = {
    val sqlContext = getSparkSqlContext()
    import sqlContext.implicits._  
    predictions.where($"rowid" === rowid).select("prediction").rdd.map(r => r.getDouble(1)).collect().map { item =>
        (item.toString(), 1.0)}.toSeq    
  }
  
  def RandomForestRegressorModel(valuePreparer:ValuePreparer = prepareValueTrain, sparkTyper:Type => DataType = getSparkType)( query:Operator, db:Database, predictionCol:String): PipelineModel = {
    val training = prepareData(query, db, valuePreparer, sparkTyper).na.drop()//.withColumn("label", toLabel($"topic".like("sci%"))).cache
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
      .setLabelCol(predictionCol)
      .setFeaturesCol("indexedFeatures")
     
    // Chain indexer and forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(assembler, featureIndexer, rf))
      
    pipeline.fit(training)
  }
  
  def GradientBoostedTreeRegressorModel(valuePreparer:ValuePreparer = prepareValueTrain, sparkTyper:Type => DataType = getSparkType)( query:Operator, db:Database, predictionCol:String): PipelineModel = {
    val training = prepareData(query, db, valuePreparer, sparkTyper).na.drop()//.withColumn("label", toLabel($"topic".like("sci%"))).cache
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
      .setLabelCol(predictionCol)
      .setFeaturesCol("indexedFeatures")
      .setMaxIter(10)
      
    // Chain indexer and forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(assembler, featureIndexer, rf))
     
    pipeline.fit(training)
  }
  
  
  def DecisionTreeRegressorModel(valuePreparer:ValuePreparer = prepareValueTrain, sparkTyper:Type => DataType = getSparkType)( query:Operator, db:Database, predictionCol:String): PipelineModel = {
    val training = prepareData(query, db, valuePreparer, sparkTyper).na.drop()//.withColumn("label", toLabel($"topic".like("sci%"))).cache
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
      .setLabelCol(predictionCol)
      .setFeaturesCol("indexedFeatures")
      
    // Chain indexer and forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(assembler, featureIndexer, rf))
      
    pipeline.fit(training)
  }
  
  def LinearRegressorModel(valuePreparer:ValuePreparer = prepareValueTrain, sparkTyper:Type => DataType = getSparkType)( query:Operator, db:Database, predictionCol:String): PipelineModel = {
    val training = prepareData(query, db, valuePreparer, sparkTyper).na.drop()//.withColumn("label", toLabel($"topic".like("sci%"))).cache
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
      .setLabelCol(predictionCol)
      .setFeaturesCol("features")
      
    // Chain indexer and forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(assembler, rf))
     
    pipeline.fit(training)
  }
  
  def GeneralizedLinearRegressorModel(valuePreparer:ValuePreparer = prepareValueTrain, sparkTyper:Type => DataType = getSparkType)( query:Operator, db:Database, predictionCol:String): PipelineModel = {
    val training = prepareData(query, db, valuePreparer, sparkTyper).na.drop()//.withColumn("label", toLabel($"topic".like("sci%"))).cache
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
      .setLabelCol(predictionCol)
      .setFeaturesCol("features")
      
    // Chain indexer and forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(assembler, rf))
   
    pipeline.fit(training)
  }
}