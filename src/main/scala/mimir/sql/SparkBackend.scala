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
import org.apache.spark.SparkFiles
import java.io.File
import mimir.util.HadoopUtils
import org.apache.spark.launcher.SparkLauncher
import org.apache.hadoop.fs.Path
import mimir.Mimir
import mimir.util.SparkUtils

class SparkBackend extends RABackend{
  var sparkSql : SQLContext = null
  ExperimentalOptions.enable("remoteSpark")
  val (sparkHost, sparkPort, hdfsPort, useHDFSHostnames, overwriteHDFSFiles) = Mimir.conf match {
    case null => (/*"128.205.71.102"*/"spark-master.local", "7077", "8020", "false", false)
    case x => (x.sparkHost, x.sparkPort, "8020", "false", false)
  }
  val remoteSpark = ExperimentalOptions.isEnabled("remoteSpark")
  def open(): Unit = {
    sparkSql = sparkSql match {
      case null => {
        val conf = if(remoteSpark){
          new SparkConf().setMaster(s"spark://$sparkHost:$sparkPort")
            .set("fs.hdfs.impl",classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
            .set("spark.submit.deployMode","client")
            .set("spark.ui.port","4041")
            .setAppName("Mimir")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .registerKryoClasses(SparkUtils.getSparkKryoClasses())
        }
        else{
          new SparkConf().setMaster("local[*]")
            .setAppName("Mimir")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .registerKryoClasses(SparkUtils.getSparkKryoClasses())
        }
        if(ExperimentalOptions.isEnabled("GPROM-BACKEND")){
          sys.props.get("os.name") match {
      	  	case Some(osname) if osname.startsWith("Mac OS X") => conf.set("spark.executorEnv.DYLD_INSERT_LIBRARIES",System.getProperty("java.home")+"/lib/libjsig.dylib")
      	  	case Some(otherosname) => conf.set("spark.executorEnv.LD_PRELOAD",System.getProperty("java.home")+"/lib/"+System.getProperty("os.arch")+"/libjsig.so")
      	  	case None => println("No os name so no preload!")
          }
        }
        val sparkCtx = SparkContext.getOrCreate(conf)//new SparkContext(conf)
        val dmode = sparkCtx.deployMode
        if(remoteSpark){
          sparkCtx.hadoopConfiguration.set("fs.hdfs.impl",classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
          sparkCtx.hadoopConfiguration.set("dfs.client.use.datanode.hostname",useHDFSHostnames)
          //sparkCtx.addJar("https://maven.mimirdb.info/info/mimirdb/mimir-core_2.11/0.2/mimir-core_2.11-0.2.jar")
          sparkCtx.addJar("https://odin.cse.buffalo.edu/assets/mimir-core_2.11-0.2.jar")
          sparkCtx.addJar("https://odin.cse.buffalo.edu/assets/scala-logging-slf4j_2.11-2.1.2.jar")
          sparkCtx.addJar("https://odin.cse.buffalo.edu/assets/scala-logging-api_2.11-2.1.2.jar")
          sparkCtx.addJar("https://odin.cse.buffalo.edu/assets/play-json_2.11-2.5.0-M2.jar")
          sparkCtx.addJar("https://odin.cse.buffalo.edu/assets/gt-referencing-16.2.jar")
          //sparkCtx.addJar("http://central.maven.org/maven2/mysql/mysql-connector-java/5.1.6/mysql-connector-java-5.1.6.jar")
          sparkCtx.hadoopConfiguration.set("fs.defaultFS", s"hdfs://$sparkHost:$hdfsPort")
        }
        println(s"apache spark: ${sparkCtx.version}  remote: $remoteSpark deployMode: $dmode")
        new SQLContext(sparkCtx)
      }
      case sparkSqlCtx => sparkSqlCtx
    }
    mimir.ml.spark.SparkML(sparkSql)
  }

  def materializeView(name:String): Unit = {
    if(sparkSql == null) throw new Exception("There is no spark context")
    sparkSql.table(name).persist().count()
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
      new Dataset[Row](sparkSql.sparkSession, sparkOper, RowEncoder(qe.analyzed.schema))
    } catch {
      case t: Throwable => {
        println("-------------------------> Exception Executing Spark Op: " + t.toString() + "\n" + t.getStackTrace.mkString("\n"))
        println("------------------------ spark op --------------------------")
        println(sparkOper)
        println("------------------------------------------------------------")
        throw t
      }
    }
  }
  
  
  
  def readDataSource(name:String, format:String, options:Map[String, String], schema:Option[Seq[(String, Type)]], load:Option[String]) = {
    if(sparkSql == null) throw new Exception("There is no spark context")
    val dsFormat = sparkSql.read.format(format)
    val dsOptions = options.toSeq.foldLeft(dsFormat)( (ds, opt) => ds.option(opt._1, opt._2))
    val dsSchema = schema match {
      case None => dsOptions
      case Some(customSchema) => dsOptions.schema(OperatorTranslation.mimirSchemaToStructType(customSchema))
    }
    (load match {
      case None => dsSchema.load
      case Some(ldf) => {
        if(remoteSpark){
          val fileName = ldf.split(File.separator).last
          val hdfsHome = HadoopUtils.getHomeDirectoryHDFS(sparkSql.sparkSession.sparkContext)
          println("Copy File To HDFS: " +hdfsHome+File.separator+fileName)
          //if(!HadoopUtils.fileExistsHDFS(sparkSql.sparkSession.sparkContext, fileName))
          HadoopUtils.writeToHDFS(sparkSql.sparkSession.sparkContext, fileName, new File(ldf), overwriteHDFSFiles)
          dsSchema.load(s"$hdfsHome/$fileName")
        }
        else dsSchema.load(ldf)
        
      }
    })/*.toDF(df.columns.map(_.toUpperCase): _*)*/.persist().createOrReplaceTempView(name)
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
  def dateType: Type = TDate()
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