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
import mimir.provenance.Provenance
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.TimestampType

object SparkML {
  type SparkModel = PipelineModel
  case class SparkModelGeneratorParams(db:Database, predictionCol:String, handleInvalid:String /*keep, skip, error*/) 
  type SparkModelGenerator = SparkModelGeneratorParams => PipelineModel
  var sc: Option[SparkContext] = None
  var sqlCtx : Option[SQLContext] = None
  def apply(spark:SQLContext) = {
    sc = Some(spark.sparkSession.sparkContext)
    sqlCtx = Some(spark)
  }
  def getDataFrameWithProvFromQuery(db:Database, query:Operator) : (Seq[(String, BaseType)], DataFrame) = {
    val prov = if(ExperimentalOptions.isEnabled("GPROM-PROVENANCE")
        && ExperimentalOptions.isEnabled("GPROM-BACKEND"))
      { db.gpromTranslator.compileProvenanceWithGProM(query) }
      else { Provenance.compile(query) }
    val oper           = prov._1
    val provenanceCols = prov._2
    val operWProv = Project(query.columnNames.map { name => ProjectArg(name, Var(name)) } :+
        ProjectArg(Provenance.rowidColnameBase, 
            Function(Provenance.mergeRowIdFunction, provenanceCols.map( Var(_) ) )), oper )
    val dfPreOut = db.backend.execute(operWProv)
    val dfOut = dfPreOut.schema.fields.filter(col => Seq(DateType, TimestampType).contains(col.dataType)).foldLeft(dfPreOut)((init, cur) => init.withColumn(cur.name,init(cur.name).cast(LongType)) )
    (db.typechecker.schemaOf(operWProv).map(el => el._2 match {
      case TDate() | TTimestamp() => (el._1, TInt())
      case _ => (el._1, db.types.rootType(el._2))
    }), dfOut)
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
  
  def fillNullValues(df:DataFrame) : DataFrame = {
    df.schema.fields.foldLeft(df)((init, curr) => {
      curr.dataType match {
        case LongType => init.na.fill(0L, Seq(curr.name))
        case IntegerType => init.na.fill(0L, Seq(curr.name))
        case FloatType => init.na.fill(0.0, Seq(curr.name))
        case DoubleType => init.na.fill(0.0, Seq(curr.name))
        case ShortType => init.na.fill(0.0, Seq(curr.name))
        case DateType => init.na.fill(0, Seq(curr.name))
        case BooleanType => init.na.fill(0, Seq(curr.name))
        case TimestampType => init.na.fill(0L, Seq(curr.name))
        case x => init.na.fill("", Seq(curr.name))
      }
    })
  }
  
  def applyModelDB(model : PipelineModel, query : Operator, db:Database, dfTransformer:Option[DataFrameTransformer] = None) : DataFrame = {
    val data = db.query(query)(results => {
      results.toList.map(row => row.provenance +: row.tupleSchema.zip(row.tuple).filterNot(_._1._1.equalsIgnoreCase("rowid")).unzip._2)
    })
    applyModel(
      model, 
      ("rowid", TString()) +:db.typechecker.baseSchemaOf(query).filterNot(_._1.equalsIgnoreCase("rowid")), 
      data,
      db.sparkTranslator, 
      dfTransformer
    )
  }
  
  def applyModel( model : PipelineModel, cols:Seq[(String, BaseType)], testData : List[Seq[PrimitiveValue]], sparkTranslator: OperatorTranslation, dfTransformer:Option[DataFrameTransformer] = None): DataFrame = {
    val sqlContext = getSparkSqlContext()
    import sqlContext.implicits._
    val modDF = dfTransformer.getOrElse((df:DataFrame) => df)
    model.transform(modDF(sqlContext.createDataFrame(
      getSparkSession().parallelize(testData.map( row => {
        Row(row.zip(cols).map(value => sparkTranslator.mimirExprToSparkExpr(null, value._1)):_*)
      })), StructType(cols.toList.map(col => StructField(col._1, OperatorTranslation.getSparkType(col._2), true))))))
  }
  
  def applyModel( model : PipelineModel, inputDF:DataFrame): DataFrame = {//inputPlan:LogicalPlan): DataFrame = {
    model.transform(inputDF)
  }
  
  def extractPredictions(model : PipelineModel, predictions:DataFrame, maxPredictions:Int = 5) : Seq[(String, (String, Double))]  
  
  def extractPredictionsForRow(model : PipelineModel, predictions:DataFrame, rowid:String, maxPredictions:Int = 5) : Seq[(String, Double)]
   
  def getNative(value:PrimitiveValue, t:BaseType): Any = {
    value match {
      case NullPrimitive() => t match {
        case TInt() => 0L
        case TFloat() => new java.lang.Double(0.0)
        case TDate() => OperatorTranslation.defaultDate
        case TString() => ""
        case TBool() => new java.lang.Boolean(false)
        case TRowId() => ""
        case TType() => "any"
        case TAny() => ""
        case TTimestamp() => OperatorTranslation.defaultTimestamp
        case TInterval() => OperatorTranslation.defaultInterval
      }
      case RowIdPrimitive(s) => s
      case StringPrimitive(s) => s
      case IntPrimitive(i) => i
      case FloatPrimitive(f) => new java.lang.Double(f)
      case BoolPrimitive(b) => new java.lang.Boolean(b)
      case ts@TimestampPrimitive(y,m,d,h,mm,s,ms) => SparkUtils.convertTimestamp(ts)
      case dt@DatePrimitive(y,m,d) => SparkUtils.convertDate(dt)
      case IntervalPrimitive(period) => period
      case TypePrimitive(t) => t
    }
  }
}
