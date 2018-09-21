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
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.catalyst.expressions.Literal
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
import com.typesafe.scalalogging.slf4j.LazyLogging
import mimir.util.S3Utils
import org.apache.spark.sql.execution.command.DropTableCommand
import org.apache.spark.sql.catalyst.TableIdentifier
import mimir.algebra.spark.function.SparkFunctions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.TimestampType
import mimir.algebra.function.FunctionRegistry
import mimir.algebra.function.AggregateRegistry


class SparkBackend(override val database:String, maintenance:Boolean = false) extends RABackend(database) 
  with BackendWithSparkContext
  with LazyLogging
{
  
  var sparkSql : SQLContext = null
  //ExperimentalOptions.enable("remoteSpark")
  val (sparkHost, sparkPort, hdfsPort, useHDFSHostnames, overwriteStagedFiles, overwriteJars, numPartitions, dataStagingType) = Mimir.conf match {
    case null => (/*"128.205.71.41"*/"spark-master.local", "7077", "8020", false, false, false, 8, "s3")
    case x => (x.sparkHost(), x.sparkPort(), x.hdfsPort(), x.useHDFSHostnames(), x.overwriteStagedFiles(), x.overwriteJars(), x.numPartitions(), x.dataStagingType())
  }
  val remoteSpark = ExperimentalOptions.isEnabled("remoteSpark")
  def open(): Unit = {
    logger.warn(s"Open SparkBackend: sparkHost:$sparkHost, sparkPort:$sparkPort, hdfsPort:$hdfsPort, useHDFSHostnames:$useHDFSHostnames, overwriteStagedFiles:$overwriteStagedFiles, overwriteJars:$overwriteJars, numPartitions:$numPartitions, dataStagingType:$dataStagingType")
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
            .set("spark.kryoserializer.buffer.max", "256m")
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
      	  	case None => logger.debug("No os name so no preload!")
          }
        }
        val sparkCtx = SparkContext.getOrCreate(conf)//new SparkContext(conf)
        val dmode = sparkCtx.deployMode
        if(remoteSpark){
          sparkCtx.hadoopConfiguration.set("dfs.client.use.datanode.hostname",useHDFSHostnames.toString())
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
          //HadoopUtils.writeToHDFS(sparkCtx, "aws-java-sdk-s3-1.11.355.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/com.amazonaws/aws-java-sdk-s3/jars/aws-java-sdk-s3-1.11.355.jar"), overwriteJars)
          //HadoopUtils.writeToHDFS(sparkCtx, "hadoop-aws-2.7.6.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/org.apache.hadoop/hadoop-aws/jars/hadoop-aws-2.7.6.jar"), overwriteJars)
          
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
          //sparkCtx.addJar(s"$hdfsHome/aws-java-sdk-s3-1.11.355.jar")
          //sparkCtx.addJar(s"$hdfsHome/hadoop-aws-2.7.6.jar")
          
          //sparkCtx.addJar("http://central.maven.org/maven2/mysql/mysql-connector-java/5.1.6/mysql-connector-java-5.1.6.jar")
        }
        logger.debug(s"apache spark: ${sparkCtx.version}  remote: $remoteSpark deployMode: $dmode")
        //TODO: we need to do this in a more secure way (especially vizier has python scripts that could expose this)
        val accessKeyId = System.getenv("AWS_ACCESS_KEY_ID")
        val secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY")
        val endpoint = Option(System.getenv("S3A_ENDPOINT"))
        endpoint.flatMap(ep => {sparkCtx.hadoopConfiguration.set("fs.s3a.endpoint", ep); None})
        sparkCtx.hadoopConfiguration.set("fs.s3a.access.key",accessKeyId)
        sparkCtx.hadoopConfiguration.set("fs.s3a.secret.key",secretAccessKey)
        sparkCtx.hadoopConfiguration.set("fs.s3a.path.style.access","true")
        sparkCtx.hadoopConfiguration.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        sparkCtx.hadoopConfiguration.set("com.amazonaws.services.s3.disableGetObjectMD5Validation", "true")
        sparkCtx.hadoopConfiguration.set("com.amazonaws.services.s3.disablePutObjectMD5Validation", "true")
        sparkCtx.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "true")
        sparkCtx.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", accessKeyId)
        sparkCtx.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", secretAccessKey)
        sparkCtx.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        new SQLContext(sparkCtx)
      }
      case sparkSqlCtx => sparkSqlCtx
    }
    //val dbs = sparkSql.sparkSession.catalog.listDatabases().collect()
    if(!maintenance){
      if(!sparkSql.sparkSession.catalog.databaseExists(database))//!dbs.map(_.name).contains(database))
        CreateDatabaseCommand(database, true, None, None, Map()).run(sparkSql.sparkSession)
      SetDatabaseCommand(database).run(sparkSql.sparkSession)
    }
    /*dbs.map(sdb => { 
      logger.debug(s"db: ${sdb.name}")
      sparkSql.sparkSession.catalog.listTables(sdb.name).show()
    })*/
    mimir.ml.spark.SparkML(sparkSql)
    
  }

  def registerSparkFunctions(excludedFunctions:Seq[String], fr:FunctionRegistry) = {
    val sparkFunctions = sparkSql.sparkSession.sessionState.catalog
        .listFunctions(database, "*")
    sparkFunctions.filterNot(fid => excludedFunctions.contains(fid._1.funcName.toUpperCase())).foreach{ case (fidentifier, fname) => {
          val fi = sparkSql.sparkSession.sessionState.catalog.lookupFunctionInfo(fidentifier)
          if(!fi.getClassName.startsWith("org.apache.spark.sql.catalyst.expressions.aggregate")){
            SparkFunctions.addSparkFunction(fidentifier.funcName.toUpperCase(), (inputs) => {
              val sparkInputs = inputs.map(inp => Literal(OperatorTranslation.mimirPrimitiveToSparkExternalRowValue(inp)))
              val sparkRow = InternalRow(inputs.map(inp => OperatorTranslation.mimirPrimitiveToSparkInternalRowValue(inp)):_*)
              val constructorTypes = inputs.map(inp => classOf[org.apache.spark.sql.catalyst.expressions.Expression])
              val sparkFunc = Class.forName(fi.getClassName).getDeclaredConstructor(constructorTypes:_*).newInstance(sparkInputs:_*)
                                .asInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression]
              val sparkRes = sparkFunc.eval(sparkRow)
              sparkFunc.dataType match {
                  case LongType => IntPrimitive(sparkRes.asInstanceOf[Long])
                  case IntegerType => IntPrimitive(sparkRes.asInstanceOf[Int].toLong)
                  case FloatType => FloatPrimitive(sparkRes.asInstanceOf[Float])
                  case DoubleType => FloatPrimitive(sparkRes.asInstanceOf[Double])
                  case ShortType => IntPrimitive(sparkRes.asInstanceOf[Short].toLong)
                  case DateType => SparkUtils.convertDate(sparkRes.asInstanceOf[java.sql.Date])
                  case BooleanType => BoolPrimitive(sparkRes.asInstanceOf[Boolean])
                  case TimestampType => SparkUtils.convertTimestamp(sparkRes.asInstanceOf[java.sql.Timestamp])
                  case x => StringPrimitive(sparkRes.toString())
                } 
            }, 
            (inputTypes) => {
              val inputs = inputTypes.map(inp => Literal(OperatorTranslation.getNative(NullPrimitive(), inp)).asInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression])
              val constructorTypes = inputs.map(inp => classOf[org.apache.spark.sql.catalyst.expressions.Expression])
              OperatorTranslation.getMimirType( Class.forName(fi.getClassName).getDeclaredConstructor(constructorTypes:_*).newInstance(inputs:_*)
              .asInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression].dataType)
            })
          } 
      } }
    SparkFunctions.register(fr)
  }
  
  def registerSparkAggregates(excludedFunctions:Seq[String], ar:AggregateRegistry) = {
    val sparkFunctions = sparkSql.sparkSession.sessionState.catalog
        .listFunctions(database, "*")
    sparkFunctions.filterNot(fid => excludedFunctions.contains(fid._1.funcName.toUpperCase())).flatMap{ case (fidentifier, fname) => {
          val fi = sparkSql.sparkSession.sessionState.catalog.lookupFunctionInfo(fidentifier)
          if(fi.getClassName.startsWith("org.apache.spark.sql.catalyst.expressions.aggregate")){
            Some((fidentifier.funcName.toUpperCase(), 
            (inputTypes:Seq[Type]) => {
              val inputs = inputTypes.map(inp => Literal(OperatorTranslation.getNative(NullPrimitive(), inp)).asInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression])
              val constructorTypes = inputs.map(inp => classOf[org.apache.spark.sql.catalyst.expressions.Expression])
              val dt = OperatorTranslation.getMimirType( Class.forName(fi.getClassName).getDeclaredConstructor(constructorTypes:_*).newInstance(inputs:_*)
              .asInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression].dataType)
              dt
            },
            NullPrimitive()
            ))
          } else None 
      } }.foreach(sa => {
        logger.debug("registering spark aggregate: " + sa._1)
        ar.register(sa._1, sa._2,sa._3)
       })    
  }
  
  def materializeView(name:String): Unit = {
    if(sparkSql == null) throw new Exception("There is no spark context")
    sparkSql.table(name).persist().count()
  }
  
  def createTable(tableName:String, oper:Operator) = {
    val df = execute(oper)
    df.persist().createOrReplaceTempView(tableName)
    df.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
  }
  
  def execute(compiledOp: Operator): DataFrame = {
    var sparkOper:LogicalPlan = null
    try {
      logger.trace("------------------------ mimir op --------------------------")
      logger.trace(s"$compiledOp")
      logger.trace("------------------------------------------------------------")
      if(sparkSql == null) throw new Exception("There is no spark context")
      sparkOper = OperatorTranslation.mimirOpToSparkOp(compiledOp)
      logger.trace("------------------------ spark op --------------------------")
      logger.trace(s"$sparkOper")
      logger.trace("------------------------------------------------------------")
      val qe = sparkSql.sparkSession.sessionState.executePlan(sparkOper)
      qe.assertAnalyzed()
      new Dataset[Row](sparkSql.sparkSession, sparkOper, RowEncoder(qe.analyzed.schema))
    } catch {
      case t: Throwable => {
        logger.error("-------------------------> Exception Executing Spark Op: " + t.toString() + "\n" + t.getStackTrace.mkString("\n"))
        logger.error("------------------------ spark op --------------------------")
        logger.error(s"$sparkOper")
        logger.error("------------------------------------------------------------")
        throw t
      }
    }
  }
  
  def dropTable(table:String): Unit = {
    DropTableCommand(TableIdentifier(table, Option(database)), true, false, true).run(sparkSql.sparkSession)
  }
  
  def dropDB():Unit = {
    DropDatabaseCommand(database, true, true).run(sparkSql.sparkSession)
    val hdfsHome = HadoopUtils.getHomeDirectoryHDFS(sparkSql.sparkSession.sparkContext)
    HadoopUtils.deleteFromHDFS( sparkSql.sparkSession.sparkContext, s"${hdfsHome}/metastore_db/$database")
  }

  
  def readDataSource(name:String, format:String, options:Map[String, String], schema:Option[Seq[(String, Type)]], load:Option[String]) = {
    if(sparkSql == null) throw new Exception("There is no spark context")
    def copyToS3(file:String): String = {
      val accessKeyId = System.getenv("AWS_ACCESS_KEY_ID")
      val secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY")
      val endpoint = System.getenv("S3_ENDPOINT") match {
        case null => None
        case "" => None
        case x => Some(x)
      }
      val s3client = S3Utils.authenticate(accessKeyId, secretAccessKey, "us-east-1", endpoint)
      var relPath = file.replaceFirst("https?://", "").replace(new File("").getAbsolutePath + File.separator, "")
      while(relPath.startsWith(File.separator))
        relPath = relPath.replaceFirst(File.separator, "")
      logger.debug(s"upload to s3: $file -> $relPath")
      //this is slower but will work with URLs and local files
      S3Utils.copyToS3("mimir-test-data", file, relPath, s3client, overwriteStagedFiles)
      //this is faster but does not work with URLs - only local files
      //S3Utils.uploadFile("mimir-test-data", file, relPath, s3client, overwriteStagedFiles)
      relPath
    }
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
          if(ldf.startsWith("s3n:/") || ldf.startsWith("s3a:/")){
            dsSchema.load(ldf)
          }
          else{
            if(dataStagingType.equalsIgnoreCase("s3")){
              dsSchema.load("s3n://mimir-test-data/"+copyToS3(ldf))
            }
            else{
              val hdfsHome = HadoopUtils.getHomeDirectoryHDFS(sparkSql.sparkSession.sparkContext)
              logger.debug("Copy File To HDFS: " +hdfsHome+File.separator+fileName)
              //if(!HadoopUtils.fileExistsHDFS(sparkSql.sparkSession.sparkContext, fileName))
              HadoopUtils.writeToHDFS(sparkSql.sparkSession.sparkContext, fileName, new File(ldf), overwriteStagedFiles)
              logger.debug("... done\n")
              pathIfCSV = s"""(path "$hdfsHome/$fileName", """
              dsSchema.load(s"$hdfsHome/$fileName")
            }
          }
        }
        else {
          if(ldf.startsWith("s3n:/") || ldf.startsWith("s3a:/") || !dataStagingType.equalsIgnoreCase("s3")){
            dsSchema.load(ldf)
          }
          else {
            dsSchema.load("s3n://mimir-test-data/"+copyToS3(ldf))
          }
        }
        
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
  def specializeQuery(q: Operator, db: mimir.Database): Operator = {
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