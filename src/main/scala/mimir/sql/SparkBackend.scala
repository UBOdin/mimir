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
import org.apache.spark.sql.execution.command.CreateViewCommand
import org.apache.spark.sql.execution.command.PersistedView
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.command.SetDatabaseCommand
import org.apache.spark.sql.execution.command.CreateDatabaseCommand
import org.apache.spark.sql.execution.command.DropDatabaseCommand
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext


class SparkBackend(override val database:String) extends RABackend(database) with BackendWithSparkContext{
  var sparkSql : SQLContext = null
  //ExperimentalOptions.enable("remoteSpark")
  val (sparkHost, sparkPort, hdfsPort, useHDFSHostnames, overwriteHDFSFiles, overwriteJars, numPartitions) = Mimir.conf match {
    case null => (/*"128.205.71.102"*/"spark-master.local", "7077", "8020", "false", false, false, 8)
    case x => (x.sparkHost(), x.sparkPort(), "8020", "false", false, false, 8)
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
            .set("spark.driver.cores","4")
            .set("spark.driver.memory","8g")
            .set("spark.executor.memory","8g")
            .set("spark.sql.catalogImplementation", "hive")
            .set("spark.sql.shuffle.partitions", s"$numPartitions")//TODO: make this the number of workers
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .registerKryoClasses(SparkUtils.getSparkKryoClasses())
        }
        else{
          new SparkConf().setMaster("local[*]")
            .setAppName("Mimir")
            .set("spark.sql.catalogImplementation", "hive")
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
          sparkCtx.hadoopConfiguration.set("dfs.client.use.datanode.hostname",useHDFSHostnames)
          sparkCtx.hadoopConfiguration.set("fs.hdfs.impl",classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
          sparkCtx.hadoopConfiguration.set("fs.defaultFS", s"hdfs://$sparkHost:$hdfsPort")
          val hdfsHome = HadoopUtils.getHomeDirectoryHDFS(sparkCtx)
          sparkCtx.hadoopConfiguration.set("spark.sql.warehouse.dir",s"${hdfsHome}/metastore_db")
          sparkCtx.hadoopConfiguration.set("hive.metastore.warehouse.dir",s"${hdfsHome}/metastore_db")
          HadoopUtils.writeToHDFS(sparkCtx, "mimir-core_2.11-0.2.jar", new File(s"${System.getProperty("user.home")}/.m2/repository/info/mimirdb/mimir-core_2.11/0.2/mimir-core_2.11-0.2.jar"), overwriteJars)
          HadoopUtils.writeToHDFS(sparkCtx, "scala-logging-slf4j_2.11-2.1.2.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/com.typesafe.scala-logging/scala-logging-api_2.11/jars/scala-logging-api_2.11-2.1.2.jar"), overwriteJars)
          HadoopUtils.writeToHDFS(sparkCtx, "scala-logging-api_2.11-2.1.2.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/com.typesafe.scala-logging/scala-logging-slf4j_2.11/jars/scala-logging-slf4j_2.11-2.1.2.jar"), overwriteJars)
          HadoopUtils.writeToHDFS(sparkCtx, "play-json_2.11-2.5.0-M2.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/com.typesafe.play/play-json_2.11/jars/play-json_2.11-2.5.0-M2.jar"), overwriteJars)
          HadoopUtils.writeToHDFS(sparkCtx, "jsr-275-0.9.1.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/javax.measure/jsr-275/jars/jsr-275-0.9.1.jar"), overwriteJars)
          HadoopUtils.writeToHDFS(sparkCtx, "GeographicLib-Java-1.44.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/net.sf.geographiclib/GeographicLib-Java/jars/GeographicLib-Java-1.44.jar"), overwriteJars)
          HadoopUtils.writeToHDFS(sparkCtx, "jai_core-1.1.3.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/javax.media/jai_core/jars/jai_core-1.1.3.jar"), overwriteJars)
          HadoopUtils.writeToHDFS(sparkCtx, "gt-opengis-16.2.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/org.geotools/gt-opengis/jars/gt-opengis-16.2.jar"), overwriteJars)
          HadoopUtils.writeToHDFS(sparkCtx, "gt-metadata-16.2.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/org.geotools/gt-metadata/jars/gt-metadata-16.2.jar"), overwriteJars)
          HadoopUtils.writeToHDFS(sparkCtx, "gt-referencing-16.2.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/org.geotools/gt-referencing/jars/gt-referencing-16.2.jar"), overwriteJars)
          //sparkCtx.addJar("https://maven.mimirdb.info/info/mimirdb/mimir-core_2.11/0.2/mimir-core_2.11-0.2.jar")
          sparkCtx.addJar(s"$hdfsHome/mimir-core_2.11-0.2.jar")
          sparkCtx.addJar(s"$hdfsHome/scala-logging-slf4j_2.11-2.1.2.jar")                                                         
          sparkCtx.addJar(s"$hdfsHome/scala-logging-api_2.11-2.1.2.jar")       
          sparkCtx.addJar(s"$hdfsHome/play-json_2.11-2.5.0-M2.jar")  
          sparkCtx.addJar(s"$hdfsHome/jsr-275-0.9.1.jar")                                     
          sparkCtx.addJar(s"$hdfsHome/jai_core-1.1.3.jar")
          sparkCtx.addJar(s"$hdfsHome/GeographicLib-Java-1.44.jar")
          sparkCtx.addJar(s"$hdfsHome/gt-opengis-16.2.jar")
          sparkCtx.addJar(s"$hdfsHome/gt-metadata-16.2.jar")
          sparkCtx.addJar(s"$hdfsHome/gt-referencing-16.2.jar")
          
          //sparkCtx.addJar("http://central.maven.org/maven2/mysql/mysql-connector-java/5.1.6/mysql-connector-java-5.1.6.jar")
        }
        println(s"apache spark: ${sparkCtx.version}  remote: $remoteSpark deployMode: $dmode")
        new SQLContext(sparkCtx)
      }
      case sparkSqlCtx => sparkSqlCtx
    }
    //val dbs = sparkSql.sparkSession.catalog.listDatabases().collect()
    if(!sparkSql.sparkSession.catalog.databaseExists(database))//!dbs.map(_.name).contains(database))
      CreateDatabaseCommand(database, true, None, None, Map()).run(sparkSql.sparkSession)
    SetDatabaseCommand(database).run(sparkSql.sparkSession)
    /*dbs.map(sdb => { 
      println(s"db: ${sdb.name}")
      sparkSql.sparkSession.catalog.listTables(sdb.name).show()
    })*/
    mimir.ml.spark.SparkML(sparkSql)
  }

  def materializeView(name:String): Unit = {
    if(sparkSql == null) throw new Exception("There is no spark context")
    sparkSql.table(name).persist().count()
  }
  
  def createTable(tableName:String, oper:Operator) = {
    val df = execute(oper)
    df.write.mode(SaveMode.Overwrite).saveAsTable(tableName)//.persist().createOrReplaceTempView(tableName)
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
  
  def dropDB():Unit = {
    DropDatabaseCommand(database, true, true).run(sparkSql.sparkSession)
    val hdfsHome = HadoopUtils.getHomeDirectoryHDFS(sparkSql.sparkSession.sparkContext)
    HadoopUtils.deleteFromHDFS( sparkSql.sparkSession.sparkContext, s"${hdfsHome}/metastore_db/database")
  }

  
  def readDataSource(name:String, format:String, options:Map[String, String], schema:Option[Seq[(String, Type)]], load:Option[String]) = {
    if(sparkSql == null) throw new Exception("There is no spark context")
    var  pathIfCSV = "("
    val dsFormat = sparkSql.read.format(format) 
    val dsOptions = options.toSeq.foldLeft(dsFormat)( (ds, opt) => ds.option(opt._1, opt._2))
    val dsSchema = schema match {
      case None => dsOptions
      case Some(customSchema) => dsOptions.schema(OperatorTranslation.mimirSchemaToStructType(customSchema))
    }
    val df = (load match {
      case None => dsSchema.load
      case Some(ldf) => {
        if(remoteSpark){
          val fileName = ldf.split(File.separator).last
          val hdfsHome = HadoopUtils.getHomeDirectoryHDFS(sparkSql.sparkSession.sparkContext)
          print("Copy File To HDFS: " +hdfsHome+File.separator+fileName)
          //if(!HadoopUtils.fileExistsHDFS(sparkSql.sparkSession.sparkContext, fileName))
          HadoopUtils.writeToHDFS(sparkSql.sparkSession.sparkContext, fileName, new File(ldf), overwriteHDFSFiles)
          print("... done\n")
          pathIfCSV = s"""(path "$hdfsHome/$fileName", """
          dsSchema.load(s"$hdfsHome/$fileName")
        }
        else dsSchema.load(ldf)
        
      }
    })/*.toDF(df.columns.map(_.toUpperCase): _*)*/  //.persist().createOrReplaceTempView(name)
    
    // attempt 1 ---------------------
    //try using builtin save funcionality to create persistent table
    df.persist().createOrReplaceTempView(name) 
    df.write.mode(SaveMode.ErrorIfExists).saveAsTable(name) // but resulting table ends up being empty
   
    
    // attempt 2 ---------------------
    //try makting temp view then create a persistent view of it
    /*df.persist().createOrReplaceTempView(name)                          
    //sparkSql.sparkSession.table(name)                              
    val tableIdentifier = try {
      sparkSql.sparkSession.sessionState.sqlParser.parseTableIdentifier(name)
    } catch {
      case _: Exception => throw new RAException(s"Invalid view name: name")
    }
    CreateViewCommand(
      name = tableIdentifier,
      userSpecifiedColumns = Nil,
      comment = None,
      properties = Map.empty,
      originalText = Option(s"CREATE VIEW $name AS SELECT * FROM $name"),
      child = df.queryExecution.logical,
      allowExisting = false,
      replace = true,
      viewType = PersistedView).run(sparkSql.sparkSession)
    */
     
    // attempt 3 ---------------------
    //try creating the table from the csv file then creating a persistent view
    /*val createSql = s"CREATE TABLE ${name}_tmp (${df.schema.fields.map(schee => s"${schee.name} ${schee.dataType.sql}").mkString(",")}) USING $format OPTIONS ${options.map(opt => s"""${opt._1} "${opt._2}" """).mkString(pathIfCSV, ",", ")")}"
    sparkSql.sql(createSql)
    val tableIdentifier = try {                                                                             
      sparkSql.sparkSession.sessionState.sqlParser.parseTableIdentifier(name)     
    } catch {                                                                                                                      
      case _: Exception => throw new RAException(s"Invalid view name: name")                                                  
    }                     
    CreateViewCommand(                                                                        
      name = tableIdentifier,                                 
      userSpecifiedColumns = df.schema.fields.map(schee => (s"${schee.name}", None)),
      comment = None,                                                             
      properties = Map.empty,                                     
      originalText = Option(s"CREATE VIEW $name AS SELECT * FROM ${name}_tmp"),                                
      child = df.queryExecution.logical,                                                             
      allowExisting = false,                                                             
      replace = true,                                                   
      viewType = PersistedView).run(sparkSql.sparkSession) 
    */  
      
   
    // attempt 4 ---------------------
    //this is the workaround in the docs @ https://www.cloudera.com/documentation/enterprise/release-notes/topics/cdh_rn_spark_ki.html#ki_sparksql_dataframe_saveastable
    /*df.persist().createOrReplaceTempView(s"${name}_tmp")                         
    val createSql = s"CREATE TABLE ${name} (${df.schema.fields.map(schee => s"${schee.name} ${schee.dataType.sql}").mkString(",")}) STORED AS parquet"
    sparkSql.sql(createSql)
    sparkSql.sql(s"INSERT INTO TABLE $name SELECT * FROM ${name}_tmp")
    //sparkSql.sparkSession.table(name).show() //this is empty - wtf 
    */
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
    val path = new File("metastore_db/dbex.lck")
    path.delete()
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
  
  def getSparkContext():SQLContext = sparkSql
}

trait BackendWithSparkContext {
  def getSparkContext():SQLContext
}