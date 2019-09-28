package mimir.exec.spark

import org.apache.spark.sql.{ 
  SQLContext,
  Dataset,
  Row,
  DataFrame,
  SparkSession,
  SaveMode
}
import org.apache.spark.sql.execution.command.{
  CreateViewCommand,
  PersistedView,
  SetDatabaseCommand,
  CreateDatabaseCommand,
  DropDatabaseCommand
}
import org.apache.spark.sql.types.{
  DataType,
  LongType,
  IntegerType,
  FloatType,
  DoubleType,
  ShortType,
  DateType,
  BooleanType,
  TimestampType,
  StringType
}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
  Literal
}
import org.apache.spark.{SparkContext, SparkConf}
import com.typesafe.scalalogging.slf4j.LazyLogging
import java.io.File
import mimir.util.FileUtils

import mimir.algebra._
import mimir.algebra.function.{SparkFunctions, AggregateRegistry}
import mimir.{Database, MimirConfig}
import mimir.util.{ExperimentalOptions, SparkUtils, HadoopUtils}
import java.net.URL
import scala.sys.process._
import java.io.FileInputStream
import java.nio.file.Paths
import java.net.InetAddress
    

object MimirSpark
  extends LazyLogging
{
  private val SHEET_CRED_FILE = "api-project-378720062738-5923e0b6125f"


  private var sparkSql: SQLContext = null
  private lazy val s3AccessKey = Option(System.getenv("AWS_ACCESS_KEY_ID"))
  private lazy val s3SecretKey = Option(System.getenv("AWS_SECRET_ACCESS_KEY"))
  private lazy val s3AEndpoint  = Option(System.getenv("S3A_ENDPOINT"))
  private lazy val envHasS3Keys = !s3AccessKey.isEmpty && !s3SecretKey.isEmpty
  def remoteSpark = ExperimentalOptions.isEnabled("remoteSpark")
  def localSpark = ExperimentalOptions.isEnabled("localSpark")
  var sheetCred: String = null

  def get: SQLContext = {
    if(sparkSql == null){ 
      throw new RuntimeException("Getting spark context before it is initialized")
    }
    return sparkSql
  }

  def init(config: MimirConfig){
    logger.info(s"Init Spark: dataDir: ${config.dataDirectory()} sparkHost:${config.sparkHost()}, sparkPort:${config.sparkPort()}, hdfsPort:${config.hdfsPort()}, useHDFSHostnames:${config.useHDFSHostnames()}, overwriteStagedFiles:${config.overwriteStagedFiles()}, overwriteJars:${config.overwriteJars()}, numPartitions:${config.numPartitions()}, dataStagingType:${config.dataStagingType()}, sparkJars:${config.sparkJars()}")

    sheetCred = config.googleSheetsCredentialPath()
    var sparkHost = config.sparkHost()
    val sparkPort = config.sparkPort()
    val hdfsPort = config.hdfsPort()
      
    val conf = if(remoteSpark){
      new SparkConf().setMaster(s"spark://$sparkHost:$sparkPort")
        .set("fs.hdfs.impl",classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
        .set("spark.submit.deployMode","client")
        .set("spark.ui.port","4041")
        .setAppName("Mimir")
        .set("spark.driver.cores","4")
        .set("spark.driver.memory",  config.sparkDriverMem())
        .set("spark.executor.memory", config.sparkExecutorMem())
        .set("spark.sql.catalogImplementation", "hive")
        //.set("spark.sql.shuffle.partitions", s"$numPartitions")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryoserializer.buffer.max", "1536m")
        .set("spark.driver.port","7001")
        .set("spark.driver.host", config.mimirHost())
        .set("spark.driver.bindAddress","0.0.0.0")
        .set("spark.blockManager.port","7005")
        .set("dfs.client.use.datanode.hostname", config.useHDFSHostnames().toString())
        .set("dfs.datanode.use.datanode.hostname", config.useHDFSHostnames().toString())
        .set("spark.driver.extraJavaOptions", s"-Dderby.system.home=${config.dataDirectory()}")
        .registerKryoClasses(SparkUtils.getSparkKryoClasses())
    }
    else if(!localSpark){
      installAndRunSpark(config)
      sparkHost = InetAddress.getLocalHost.getHostAddress
      new SparkConf().setMaster(s"spark://$sparkHost:$sparkPort")
        .set("fs.hdfs.impl",classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
        .set("spark.submit.deployMode","cluster")
        .setAppName("Mimir")
        .set("spark.driver.cores","4")
        .set("spark.driver.memory",  config.sparkDriverMem())
        .set("spark.executor.memory", "4g")
        .set("spark.executor.instances", "1")
        .set("spark.executor.cores", "5")
        .set("spark.sql.catalogImplementation", "hive")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryoserializer.buffer.max", "1536m")
        .set("spark.driver.extraJavaOptions", s"-Dderby.system.home=${config.dataDirectory()}")
        .registerKryoClasses(SparkUtils.getSparkKryoClasses())
    }
    else{
      val dataDir = config.dataDirectory()
      new SparkConf().setMaster("local[*]")
        .setAppName("Mimir")
        .set("spark.sql.catalogImplementation", "hive")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.driver.extraJavaOptions", s"-Dderby.system.home=$dataDir")
        .set("spark.sql.warehouse.dir", s"${new File(dataDir).getAbsolutePath}/spark-warehouse")
        .set("spark.hadoop.javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=${new File(dataDir).getAbsolutePath}/metastore_db;create=true")
        .registerKryoClasses(SparkUtils.getSparkKryoClasses())
    }

    val sparkCtx = SparkContext.getOrCreate(conf)//new SparkContext(conf)
    //val excnt = sparkCtx.statusTracker.getExecutorInfos.length
    //sparkCtx.getConf.set("spark.executor.instances", s"${excnt-1}")
    val dmode = sparkCtx.deployMode

    if(remoteSpark || !localSpark){ 
      val credentialName = new File(sheetCred).getName
      if(remoteSpark){
        sparkCtx.hadoopConfiguration.set("dfs.client.use.datanode.hostname",  config.useHDFSHostnames.toString())
        sparkCtx.hadoopConfiguration.set("dfs.datanode.use.datanode.hostname",config.useHDFSHostnames.toString())
        sparkCtx.hadoopConfiguration.set("fs.hdfs.impl",classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
        sparkCtx.hadoopConfiguration.set("fs.defaultFS", s"hdfs://$sparkHost:$hdfsPort")
      }
      val hdfsHome = HadoopUtils.getHomeDirectoryHDFS(sparkCtx)
      val hdfsPath = if(remoteSpark) s"$hdfsHome/" else ""
      val overwriteJars = config.overwriteJars()
      sparkCtx.hadoopConfiguration.set("spark.sql.warehouse.dir",s"${hdfsPath}metastore_db")
      sparkCtx.hadoopConfiguration.set("hive.metastore.warehouse.dir",s"${hdfsPath}metastore_db")
      HadoopUtils.writeToHDFS(sparkCtx, "mimir-core_2.11-0.3-SNAPSHOT.jar", new File(s"${System.getProperty("user.home")}/.m2/repository/info/mimirdb/mimir-core_2.11/0.3-SNAPSHOT/mimir-core_2.11-0.3-SNAPSHOT.jar"), overwriteJars)
      HadoopUtils.writeToHDFS(sparkCtx, "scala-logging-slf4j_2.11-2.1.2.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/com.typesafe.scala-logging/scala-logging-api_2.11/jars/scala-logging-api_2.11-2.1.2.jar"), overwriteJars)
      HadoopUtils.writeToHDFS(sparkCtx, "scala-logging-api_2.11-2.1.2.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/com.typesafe.scala-logging/scala-logging-slf4j_2.11/jars/scala-logging-slf4j_2.11-2.1.2.jar"), overwriteJars)
      HadoopUtils.writeToHDFS(sparkCtx, "play-json_2.11-2.5.0-M2.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/com.typesafe.play/play-json_2.11/jars/play-json_2.11-2.5.0-M2.jar"), overwriteJars)
      HadoopUtils.writeToHDFS(sparkCtx, "play-functional_2.11-2.5.0-M2.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/com.typesafe.play/play-functional_2.11/jars/play-functional_2.11-2.5.0-M2.jar"), overwriteJars)
      HadoopUtils.writeToHDFS(sparkCtx, "jsr-275-0.9.1.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/javax.measure/jsr-275/jars/jsr-275-0.9.1.jar"), overwriteJars)
      HadoopUtils.writeToHDFS(sparkCtx, "postgresql-9.4-1201-jdbc41.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/org.postgresql/postgresql/jars/postgresql-9.4-1201-jdbc41.jar"), overwriteJars)
      HadoopUtils.writeToHDFS(sparkCtx, "sqlite-jdbc-3.16.1.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/org.xerial/sqlite-jdbc/jars/sqlite-jdbc-3.16.1.jar"), overwriteJars)
      HadoopUtils.writeToHDFS(sparkCtx, "spark-xml_2.11-0.5.0.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/com.databricks/spark-xml_2.11/jars/spark-xml_2.11-0.5.0.jar"), overwriteJars)
      HadoopUtils.writeToHDFS(sparkCtx, "spark-excel_2.11-0.11.0.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/com.crealytics/spark-excel_2.11/jars/spark-excel_2.11-0.11.0.jar"), overwriteJars)
      HadoopUtils.writeToHDFS(sparkCtx, "spark-google-spreadsheets_2.11-0.6.1.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/com.github.potix2/spark-google-spreadsheets_2.11/jars/spark-google-spreadsheets_2.11-0.6.1.jar"), overwriteJars)
      HadoopUtils.writeToHDFS(sparkCtx, "sparsity_2.11-1.5.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/info.mimirdb/sparsity_2.11/jars/sparsity_2.11-1.5.jar"), overwriteJars)
      HadoopUtils.writeToHDFS(sparkCtx, "fastparse_2.11-2.1.0.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/com.lihaoyi/fastparse_2.11/jars/fastparse_2.11-2.1.0.jar"), overwriteJars)
      HadoopUtils.writeToHDFS(sparkCtx, s"$credentialName",new File(s"test/data/$credentialName"), overwriteJars)
      //HadoopUtils.writeToHDFS(sparkCtx, "aws-java-sdk-s3-1.11.355.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/com.amazonaws/aws-java-sdk-s3/jars/aws-java-sdk-s3-1.11.355.jar"), overwriteJars)
      //HadoopUtils.writeToHDFS(sparkCtx, "hadoop-aws-2.7.6.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/org.apache.hadoop/hadoop-aws/jars/hadoop-aws-2.7.6.jar"), overwriteJars)
      
      //sparkCtx.addJar("https://maven.mimirdb.info/info/mimirdb/mimir-core_2.11/0.2/mimir-core_2.11-0.2.jar")
      sparkCtx.addJar(s"${hdfsPath}mimir-core_2.11-0.3-SNAPSHOT.jar")
      sparkCtx.addJar(s"${hdfsPath}scala-logging-slf4j_2.11-2.1.2.jar")                                                         
      sparkCtx.addJar(s"${hdfsPath}scala-logging-api_2.11-2.1.2.jar")       
      sparkCtx.addJar(s"${hdfsPath}play-json_2.11-2.5.0-M2.jar")  
      sparkCtx.addJar(s"${hdfsPath}play-functional_2.11-2.5.0-M2.jar")  
      sparkCtx.addJar(s"${hdfsPath}jsr-275-0.9.1.jar")                                     
      sparkCtx.addJar(s"${hdfsPath}postgresql-9.4-1201-jdbc41.jar")
      sparkCtx.addJar(s"${hdfsPath}sqlite-jdbc-3.16.1.jar")
      sparkCtx.addJar(s"${hdfsPath}spark-xml_2.11-0.5.0.jar")
      sparkCtx.addJar(s"${hdfsPath}spark-excel_2.11-0.11.0.jar")
      sparkCtx.addJar(s"${hdfsPath}spark-google-spreadsheets_2.11-0.6.1.jar")
      sparkCtx.addJar(s"${hdfsPath}sparsity_2.11-1.5.jar")
      sparkCtx.addJar(s"${hdfsPath}fastparse_2.11-2.1.0.jar")
      sparkCtx.addFile(s"${hdfsPath}$credentialName")
      
      FileUtils.getListOfFiles(config.sparkJars()).map(file => {
        if(file.getName.endsWith(".jar")){
          HadoopUtils.writeToHDFS(sparkCtx, file.getName, file, overwriteJars)
          sparkCtx.addJar(s"${hdfsPath}${file.getName}")
        }
      })
    }
    else {
      FileUtils.getListOfFiles(config.sparkJars()).map(file => {
        if(file.getName.endsWith(".jar")){
          FileUtils.addJarToClasspath(file)
          sparkCtx.addJar(file.getAbsolutePath)
        }
      })
    }

    logger.debug(s"apache spark: ${sparkCtx.version}  remote: $remoteSpark deployMode: $dmode")
    for( endpoint <- s3AEndpoint) { 
      sparkCtx.hadoopConfiguration.set("fs.s3a.endpoint", endpoint) 
    }
    if(envHasS3Keys){
      sparkCtx.hadoopConfiguration.set("fs.s3a.access.key",s3AccessKey.get)
      sparkCtx.hadoopConfiguration.set("fs.s3a.secret.key",s3SecretKey.get)
      sparkCtx.hadoopConfiguration.set("fs.s3a.path.style.access","true")
      sparkCtx.hadoopConfiguration.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
      sparkCtx.hadoopConfiguration.set("com.amazonaws.services.s3.disableGetObjectMD5Validation", "true")
      sparkCtx.hadoopConfiguration.set("com.amazonaws.services.s3.disablePutObjectMD5Validation", "true")
      sparkCtx.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "true")
      sparkCtx.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3AccessKey.get)
      sparkCtx.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3SecretKey.get)
      sparkCtx.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    } else {
      logger.debug("No S3 Access Key provided. Not configuring S3")
    }

    sparkSql = new SQLContext(sparkCtx)
  }

  def close() = {
    get // throw an error if sparkSql isn't set
    val path = new File("metastore_db/dbex.lck")
    path.delete()
    sparkSql.sparkSession.close()
    sparkSql = null
  }

  def createDatabase(name: String)
  {
    if(!get.sparkSession.catalog.databaseExists(name)) {
      CreateDatabaseCommand(name, true, None, None, Map()).run(get.sparkSession)
    }
    SetDatabaseCommand(name).run(get.sparkSession)
  }

  def dropDatabase(name: String)
  {
    DropDatabaseCommand(name, true, true).run(get.sparkSession)
    val hdfsHome = HadoopUtils.getHomeDirectoryHDFS(get.sparkSession.sparkContext)
    HadoopUtils.deleteFromHDFS( get.sparkSession.sparkContext, s"${hdfsHome}/metastore_db/$name")
  }

  def linkDBToSpark(db: Database)
  {
    createDatabase("mimir")
    val otherExcludeFuncs = Seq("NOT","AND","!","%","&","*","+","-","/","<","<=","<=>","=","==",">",">=","^","|","OR")
    registerSparkFunctions(
      db.functions.functionPrototypes.map { _._1 }.toSeq
        ++ otherExcludeFuncs.map { ID(_) }, 
      db
    )
    registerSparkAggregates(
      db.aggregates.prototypes.map { _._1 }.toSeq,
      db.aggregates
    )
  }

  def registerSparkFunctions(excludedFunctions:Seq[ID], db: Database) = {
    val fr = db.functions
    val sparkFunctions = 
        get.sparkSession
           .sessionState
           .catalog
           .listFunctions("mimir")
    sparkFunctions.filterNot(fid => excludedFunctions.contains(ID(fid._1.funcName.toLowerCase()))).foreach{ case (fidentifier, fname) => {
          val fClassName = get.sparkSession.sessionState.catalog.lookupFunctionInfo(fidentifier).getClassName
          if(!fClassName.startsWith("org.apache.spark.sql.catalyst.expressions.aggregate")){
            logger.debug("registering spark function: " + fidentifier.funcName)
            SparkFunctions.addSparkFunction(ID(fidentifier.funcName), (inputs) => {
              val sparkInputs = inputs.map(inp => Literal(RAToSpark.mimirPrimitiveToSparkExternalInlineFuncParam(inp)))
              val sparkInternal = inputs.map(inp => RAToSpark.mimirPrimitiveToSparkInternalInlineFuncParam(inp))
              val sparkRow = InternalRow(sparkInternal:_*)
              val constructorTypes = inputs.map(inp => classOf[org.apache.spark.sql.catalyst.expressions.Expression])
              val sparkFunc = Class.forName(fClassName).getDeclaredConstructor(constructorTypes:_*).newInstance(sparkInputs:_*)
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
                  case x => {
                    sparkRes match {
                      case null => NullPrimitive()
                      case _ => StringPrimitive(sparkRes.toString())
                    }
                  }
                } 
            }, 
            (inputTypes) => {
              val inputs = inputTypes.map(inp => Literal(RAToSpark.getNative(NullPrimitive(), inp)).asInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression])
              val constructorTypes = inputs.map(inp => classOf[org.apache.spark.sql.catalyst.expressions.Expression])
              RAToSpark.getMimirType( Class.forName(fClassName).getDeclaredConstructor(constructorTypes:_*).newInstance(inputs:_*)
              .asInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression].dataType)
            })
          } 
      } }
    SparkFunctions.register(fr)
  }
  
  def registerSparkAggregates(excludedFunctions:Seq[ID], ar:AggregateRegistry) = {
    val sparkFunctions = 
        get.sparkSession
           .sessionState
           .catalog
           .listFunctions("mimir")
    sparkFunctions.filterNot(fid => excludedFunctions.contains(ID(fid._1.funcName.toLowerCase()))).flatMap{ case (fidentifier, fname) => {
          val fClassName = get.sparkSession.sessionState.catalog.lookupFunctionInfo(fidentifier).getClassName
          if(fClassName.startsWith("org.apache.spark.sql.catalyst.expressions.aggregate")){
            Some((fidentifier.funcName, 
            (inputTypes:Seq[Type]) => {
              val inputs = inputTypes.map(inp => Literal(RAToSpark.getNative(NullPrimitive(), inp)).asInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression])
              val constructorTypes = inputs.map(inp => classOf[org.apache.spark.sql.catalyst.expressions.Expression])
              val dt = RAToSpark.getMimirType( Class.forName(fClassName).getDeclaredConstructor(constructorTypes:_*).newInstance(inputs:_*)
              .asInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression].dataType)
              dt
            },
            NullPrimitive()
            ))
          } else None 
      } }.foreach(sa => {
        logger.debug("registering spark aggregate: " + sa._1)
        ar.register(ID(sa._1), sa._2,sa._3)
       })    
  }
  
  def installAndRunSpark(config: MimirConfig) = {
    val dataDir = config.dataDirectory()
    val sparkDir = s"$dataDir/spark"
    val sparkDirF = new File(sparkDir)
    val dataDirF = new File(dataDir)
    if(!sparkDirF.exists()){
      sparkDirF.mkdirs()
      val sparkTarStream = new URL("https://www-us.apache.org/dist/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz").openStream();
      FileUtils.untar(sparkTarStream, sparkDir)
      //new URL("https://www-us.apache.org/dist/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz") #> new File(s"$sparkDir/spark-2.4.4-bin-hadoop2.7.tgz") !!
      //FileUtils.untar(new FileInputStream(s"$sparkDir/spark-2.4.4-bin-hadoop2.7.tgz"), sparkDir)
      val sbinFiles = listOfFiles(s"$sparkDir/spark-2.4.4-bin-hadoop2.7/sbin")
      sbinFiles.map(_.setExecutable(true))
      val binFiles = listOfFiles(s"$sparkDir/spark-2.4.4-bin-hadoop2.7/bin")
      binFiles.map(_.setExecutable(true))
      
    }
    val localIpAddress: String = InetAddress.getLocalHost.getHostAddress
    val sparkMasterProcess = Process(
      s"$sparkDir/spark-2.4.4-bin-hadoop2.7/sbin/start-master.sh",
      cwd = dataDirF,
      extraEnv = ("SPARK_MASTER_HOST", localIpAddress), ("SPARK_MASTER_PORT", s"${config.sparkPort()}")).!
      
    val sparkSlaveProcess = Process(
      Seq(s"$sparkDir/spark-2.4.4-bin-hadoop2.7/sbin/start-slave.sh", s"$localIpAddress:${config.sparkPort()}"),
      cwd = dataDirF,
      extraEnv = ("SPARK_MASTER_HOST", localIpAddress), ("SPARK_WORKER_INSTANCES", "2")).!
     
  }
  private def listOfFiles(path : String) : List[File] = {
        val paths = Paths.get(path)
        val file = paths.toFile
        if(file.isDirectory){
            file.listFiles().toList
        }else List[File]() 
    }
}
