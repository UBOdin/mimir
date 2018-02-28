package mimir.sql

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import mimir.util.ExperimentalOptions
import mimir.algebra.spark.OperatorTranslation
import org.apache.spark.sql.SQLContext
import mimir.algebra._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder

class SparkBackend extends RABackend{
  var sparkSql : SQLContext = null
  def open(): Unit = {
    sparkSql = sparkSql match {
      case null => {
        val conf = if(ExperimentalOptions.isEnabled("remoteSpark")){
          new SparkConf().setMaster("spark://localhost:7077").setAppName("Mimir")//("local[*]").setAppName("MultiClassClassification")
        }
        else{
          new SparkConf().setMaster("local[*]").setAppName("Mimir")
        }
        if(ExperimentalOptions.isEnabled("GPROM-BACKEND")){
          sys.props.get("os.name") match {
      	  	case Some(osname) if osname.startsWith("Mac OS X") => conf.set("spark.executorEnv.DYLD_INSERT_LIBRARIES",System.getProperty("java.home")+"/lib/libjsig.dylib")
      	  	case Some(otherosname) => conf.set("spark.executorEnv.LD_PRELOAD",System.getProperty("java.home")+"/lib/"+System.getProperty("os.arch")+"/libjsig.so")
      	  	case None => println("No os name so no preload!")
          }
        }
        val sparkCtx = SparkContext.getOrCreate(conf)//new SparkContext(conf)
        if(ExperimentalOptions.isEnabled("remoteSpark"))
          sparkCtx.addJar("https://maven.mimirdb.info/info/mimirdb/mimir-core_2.11/0.2/mimir-core_2.11-0.2.jar")
        println(s"apache spark: ${sparkCtx.version}")
        new SQLContext(sparkCtx)
      }
      case sparkSqlCtx => sparkSqlCtx
    }
  }

  def execute(compiledOp: Operator): DataFrame = {
    if(sparkSql == null) throw new Exception("There is no spark context")
    val sparkOper = OperatorTranslation.mimirOpToSparkOp(compiledOp)
    val qe = sparkSql.sparkSession.sessionState.executePlan(sparkOper)
    qe.assertAnalyzed()
    new Dataset[Row](sparkSql.sparkSession, sparkOper, RowEncoder(qe.analyzed.schema)).toDF()
  }
  
  
  def readDataSource(name:String, format:String, options:Map[String, String], schema:Option[Seq[(String, Type)]], load:Option[String]) = {
    if(sparkSql == null) throw new Exception("There is no spark context")
    val dsFormat = sparkSql.read.format("csv")
    val dsOptions = options.toSeq.foldLeft(dsFormat)( (ds, opt) => ds.option(opt._1, opt._2))
    val dsSchema = schema match {
      case None => dsOptions
      case Some(customSchema) => dsOptions.schema(OperatorTranslation.mimirSchemaToStructType(customSchema))
    }
    (load match {
      case None => dsSchema.load
      case Some(ldf) => dsSchema.load(ldf)
    }).persist().createOrReplaceTempView(name)
  }
  
  
  def getTableSchema(table: String): Option[Seq[(String, Type)]]
  
  
  def getAllTables(): Seq[String]
  def invalidateCache();

  def close() = {
    if(sparkSql == null) throw new Exception("There is no spark context")
    sparkSql.sparkSession.close()
    sparkSql = null
  }

  def canHandleVGTerms: Boolean = true
  def rowIdType: Type = TString()
  def dateType: Type = TString()
  def specializeQuery(q: Operator, db: Database): Operator

  def listTablesQuery: Operator
  def listAttrsQuery: Operator
}