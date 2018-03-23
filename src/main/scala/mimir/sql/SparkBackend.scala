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
import mimir.algebra.spark.OperatorTranslation
import org.apache.spark.sql.DataFrame
import mimir.Database
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

class SparkBackend extends RABackend{
  var sparkSql : SQLContext = null
  val remoteSpark = ExperimentalOptions.isEnabled("remoteSpark")
  def open(): Unit = {
    sparkSql = sparkSql match {
      case null => {
        val conf = if(remoteSpark){
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
        if(remoteSpark)
          sparkCtx.addJar("https://maven.mimirdb.info/info/mimirdb/mimir-core_2.11/0.2/mimir-core_2.11-0.2.jar")
        println(s"apache spark: ${sparkCtx.version}")
        new SQLContext(sparkCtx)
      }
      case sparkSqlCtx => sparkSqlCtx
    }
  }

  def createTable(tableName:String, oper:Operator) = {
    val df = execute(oper)
    df.persist().createOrReplaceTempView(tableName)
  }
  
  def execute(compiledOp: Operator): DataFrame = {
    var sparkOper:LogicalPlan = null
    try {
      /*println("------------------------ mimir op --------------------------")
      println(compiledOp)
      println("------------------------------------------------------------")*/
      if(sparkSql == null) throw new Exception("There is no spark context")
      sparkOper = OperatorTranslation.mimirOpToSparkOp(compiledOp)
      /*println("------------------------ spark op --------------------------")
      println(sparkOper)
      println("------------------------------------------------------------")*/
      val qe = sparkSql.sparkSession.sessionState.executePlan(sparkOper)
      qe.assertAnalyzed()
      new Dataset[Row](sparkSql.sparkSession, sparkOper, RowEncoder(qe.analyzed.schema)).toDF()
    } catch {
      case t: Throwable => {
        println("-------------------------> Exception Executing Spark Op: ")
        println("------------------------ spark op --------------------------")
        println(sparkOper)
        println("------------------------------------------------------------")
        throw t
      }
    }
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
  
  
  def getTableSchema(table: String): Option[Seq[(String, Type)]] = {
    if(sparkSql == null) throw new Exception("There is no spark context")
    if(sparkSql.sparkSession.catalog.tableExists(table))
      Some(sparkSql.sparkSession.catalog.listColumns(table).collect.map(col => (col.name, OperatorTranslation.getMimirType( OperatorTranslation.dataTypeFromHiveDataTypeString(col.dataType)))))
    else None
  }
  
  
  def getAllTables(): Seq[String] = {
    if(sparkSql == null) throw new Exception("There is no spark context")
    sparkSql.sparkSession.catalog.listTables().collect().map(table => table.name)
  }
  def invalidateCache() = {
    if(sparkSql == null) throw new Exception("There is no spark context")
    sparkSql.sparkSession.catalog.clearCache()
  }

  def close() = {
    if(sparkSql == null) throw new Exception("There is no spark context")
    sparkSql.sparkSession.close()
    sparkSql = null
  }

  def canHandleVGTerms: Boolean = true
  def rowIdType: Type = TString()
  def dateType: Type = TString()
  def specializeQuery(q: Operator, db: Database): Operator = {
    q
  }

  def listTablesQuery: Operator = {
    HardTable(
      Seq(("TABLE_NAME", TString())),
      getAllTables().map(table => Seq(StringPrimitive(table)))
    )  
  }
  
  def listAttrsQuery: Operator = {
    HardTable(Seq(
          ("TABLE_NAME", TString()), 
          ("ATTR_NAME", TString()),
          ("ATTR_TYPE", TString()),
          ("IS_KEY", TBool())
        ),
        getAllTables().flatMap { table =>
          getTableSchema(table).get.map { case (col, t) =>
            Seq(
              StringPrimitive(table),
              StringPrimitive(col),
              TypePrimitive(t),
              BoolPrimitive(false)
            )
          }
        }
      )  
  }
}