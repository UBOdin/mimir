package mimir.ml.spark

import mimir.algebra._
import mimir.Database

import org.apache.spark.sql.{SQLContext, DataFrame, Row, Dataset}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, FloatType, BooleanType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.ml.feature.Imputer
import mimir.util.ExperimentalOptions
import mimir.algebra.spark.OperatorTranslation
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import java.sql.Timestamp
import java.sql.Date
import mimir.util.SparkUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils

object SparkML {
  type SparkModel = PipelineModel
  case class SparkModelGeneratorParams(query:Operator, db:Database, predictionCol:String, handleInvalid:String /*keep, skip, error*/) 
  type SparkModelGenerator = SparkModelGeneratorParams => PipelineModel
  var sc: Option[SparkContext] = None
  var sqlCtx : Option[SQLContext] = None
  def apply(spark:SQLContext) = {
    sc = Some(spark.sparkSession.sparkContext)
    sqlCtx = Some(spark)
  }
}

abstract class SparkML {
  def getSparkSession() : SparkContext = {
      SparkML.sc match {
        case None => {
          throw new Exception("No Spark Context")
        }
        case Some(session) => session
      }
  }
  
  def getSparkSqlContext() : SQLContext = {
    SparkML.sqlCtx match {
      case None => {
        throw new Exception("No Spark Context")
      }
      case Some(ctx) => ctx
    }
  }
  
  type ValuePreparer = (PrimitiveValue, Type) => Any
  
  protected def prepareValueTrain(value:PrimitiveValue, t:Type): Any 
  
  protected def prepareValueApply(value:PrimitiveValue, t:Type): Any 
  
  protected def getSparkType(t:Type) : DataType 
  
  type DataFrameTransformer = (DataFrame) => DataFrame
  
  protected def nullValueReplacement(df:DataFrame): DataFrame = {
    import org.apache.spark.sql.functions.mean
    val imputerCols = df.schema.fields.flatMap(col => {
      if(df.filter(df(col.name).isNull).count > 0)
        col.dataType match {
          case IntegerType | LongType | DoubleType | FloatType => Some(col.name)
          case StringType => None
          case _ => None
        }
      else None
    }).toArray
    new Imputer().setInputCols(imputerCols) .setOutputCols(imputerCols).fit(df).transform(df)
  }
  
  def prepareData(query : Operator, db:Database, valuePreparer: ValuePreparer = prepareValueTrain, sparkTyper:Type => DataType = getSparkType) : DataFrame = {
    db.backend.execute(query)
    /*val schema = db.typechecker.schemaOf(query).toList
    val sqlContext = getSparkSqlContext()
    //OperatorTranslation.db = db
    //OperatorTranslation.mimirOpToDF(sqlContext, query)
    import sqlContext.implicits._
    sqlContext.createDataFrame(
      getSparkSession().parallelize(db.query(query)(results => {
        results.toList.map(row => Row((valuePreparer(row.provenance, TString() ) +: row.tuple.zip(schema).map(value => valuePreparer(value._1, value._2._2))):_*))
      })), StructType(StructField("rowid", StringType, false) :: schema.filterNot(_._1.equalsIgnoreCase("rowid")).map(col => StructField(col._1, sparkTyper(col._2), true))))
  */}
  
  def applyModelDB(model : PipelineModel, query : Operator, db:Database, valuePreparer:ValuePreparer = prepareValueApply, sparkTyper:Type => DataType = getSparkType, dfTransformer:Option[DataFrameTransformer] = None) : DataFrame = {
    val data = db.query(query)(results => {
      results.toList.map(row => row.provenance +: row.tupleSchema.zip(row.tuple).filterNot(_._1._1.equalsIgnoreCase("rowid")).unzip._2)
    })
    applyModel(model, ("rowid", TString()) +:db.typechecker.schemaOf(query).filterNot(_._1.equalsIgnoreCase("rowid")), data, valuePreparer, sparkTyper, dfTransformer)
  }
  
  def applyModel( model : PipelineModel, cols:Seq[(String, Type)], testData : List[Seq[PrimitiveValue]], valuePreparer:ValuePreparer = prepareValueApply, sparkTyper:Type => DataType = getSparkType, dfTransformer:Option[DataFrameTransformer] = None): DataFrame = {
    val sqlContext = getSparkSqlContext()
    import sqlContext.implicits._
    val modDF = dfTransformer.getOrElse((df:DataFrame) => df)
    model.transform(modDF(sqlContext.createDataFrame(
      getSparkSession().parallelize(testData.map( row => {
        Row(row.zip(cols).map(value => valuePreparer(value._1, value._2._2)):_*)
      })), StructType(cols.toList.map(col => StructField(col._1, sparkTyper(col._2), true))))))
  }
  
  def applyModel( model : PipelineModel, inputDF:DataFrame): DataFrame = {//inputPlan:LogicalPlan): DataFrame = {
    /*val sqlContext = getSparkSqlContext()
    import sqlContext.implicits._
    val qe = sqlContext.sparkSession.sessionState.executePlan(inputPlan)
    qe.assertAnalyzed()
    val dfToTf = new Dataset[Row](sqlContext.sparkSession, inputPlan, RowEncoder(qe.analyzed.schema)).toDF()*/
    model.transform(inputDF)
  }
  
  def extractPredictions(model : PipelineModel, predictions:DataFrame, maxPredictions:Int = 5) : Seq[(String, (String, Double))]  
  
  def extractPredictionsForRow(model : PipelineModel, predictions:DataFrame, rowid:String, maxPredictions:Int = 5) : Seq[(String, Double)]
   
  def getNative(value:PrimitiveValue, t:Type): Any = {
    value match {
      case NullPrimitive() => t match {
        case TInt() => 0L
        case TFloat() => new java.lang.Float(0.0)
        case TDate() => DateTimeUtils.toJavaDate(0)
        case TString() => ""
        case TBool() => new java.lang.Boolean(false)
        case TRowId() => ""
        case TType() => ""
        case TAny() => ""
        case TTimestamp() => DateTimeUtils.toJavaTimestamp(0L)
        case TInterval() => ""
        case TUser(name) => getNative(value, mimir.algebra.TypeRegistry.registeredTypes(name)._2)
        case x => ""
      }
      case RowIdPrimitive(s) => s
      case StringPrimitive(s) => s
      case IntPrimitive(i) => i
      case FloatPrimitive(f) => new java.lang.Float(f)
      case BoolPrimitive(b) => new java.lang.Boolean(b)
      case ts@TimestampPrimitive(y,m,d,h,mm,s,ms) => SparkUtils.convertTimestamp(ts)
      case dt@DatePrimitive(y,m,d) => SparkUtils.convertDate(dt)
      case x =>  x.asString
    }
  }
  
}
