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
import com.typesafe.scalalogging.LazyLogging
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
    
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.apache.spark.sql.geosparksql.expressions.ST_Point

object MimirSpark
  extends LazyLogging
{
  private val SHEET_CRED_FILE = "api-project-378720062738-5923e0b6125f"

  private var sparkSession: SparkSession = null
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
    val dataDir = config.dataDirectory()
    val scalaVersion = util.Properties.versionNumberString.substring(0,util.Properties.versionNumberString.lastIndexOf('.'))
    val sparsityVersion = sparsity.parser.SQL.getClass().getPackage().getImplementationVersion()
    
    val sparkBuilder = (if(remoteSpark){
      SparkSession.builder.master(s"spark://$sparkHost:$sparkPort")
        .config("fs.hdfs.impl",classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
        .config("spark.submit.deployMode","client")
        .config("spark.ui.port","4041")
        .appName("Mimir")
        .config("spark.driver.cores","4")
        .config("spark.driver.memory",  config.sparkDriverMem())
        .config("spark.executor.memory", config.sparkExecutorMem())
        .config("spark.sql.catalogImplementation", "hive")
        //.config("spark.sql.shuffle.partitions", s"$numPartitions")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "1536m")
        .config("spark.driver.port","7001")
        .config("spark.driver.host", config.mimirHost())
        .config("spark.driver.bindAddress","0.0.0.0")
        .config("spark.blockManager.port","7005")
        .config("dfs.client.use.datanode.hostname", config.useHDFSHostnames().toString())
        .config("dfs.datanode.use.datanode.hostname", config.useHDFSHostnames().toString())
        .config("spark.hadoop.dfs.client.use.datanode.hostname",  config.useHDFSHostnames.toString())
        .config("spark.hadoop.dfs.datanode.use.datanode.hostname",config.useHDFSHostnames.toString())
        .config("spark.hadoop.fs.hdfs.impl",classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
        .config("spark.hadoop.fs.defaultFS", s"hdfs://$sparkHost:$hdfsPort")
        .config("spark.driver.extraJavaOptions", s"-Dderby.system.home=${new File(dataDir).getAbsolutePath}")
        .config("spark.sql.warehouse.dir", s"${new File(dataDir).getAbsolutePath}/spark-warehouse")
        .config("spark.hadoop.javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=${new File(dataDir).getAbsolutePath}/metastore_db;create=true")   
    }
    else if(!localSpark){
      installAndRunSpark(config)
      sparkHost = InetAddress.getLocalHost.getHostAddress
      SparkSession.builder.master(s"spark://$sparkHost:$sparkPort")
        .config("fs.hdfs.impl",classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
        .config("spark.submit.deployMode","cluster")
        .appName("Mimir")
        .config("spark.driver.cores","4")
        .config("spark.driver.memory",  config.sparkDriverMem())
        .config("spark.executor.memory", "8g")
        .config("spark.executor.instances", "1")
        //.config("spark.executor.cores", "5")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "1536m")
        .config("spark.driver.extraJavaOptions", s"-Dderby.system.home=${new File(dataDir).getAbsolutePath}")
        .config("spark.sql.warehouse.dir", s"${new File(dataDir).getAbsolutePath}/spark-warehouse")
        .config("spark.hadoop.javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=${new File(dataDir).getAbsolutePath}/metastore_db;create=true")   
    }
    else{
      SparkSession.builder.master("local[*]")
        .appName("Mimir")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.driver.extraJavaOptions", s"-Dderby.system.home=$dataDir")
        .config("spark.sql.warehouse.dir", s"${new File(dataDir).getAbsolutePath}/spark-warehouse")
        .config("spark.hadoop.javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=${new File(dataDir).getAbsolutePath}/metastore_db;create=true")
    }).config(new SparkConf().registerKryoClasses(SparkUtils.getSparkKryoClasses()))
    sparkSession = sparkBuilder.getOrCreate
    val sparkCtx = sparkSession.sparkContext////new SparkContext(conf)
    //val excnt = sparkCtx.statusTracker.getExecutorInfos.length
    //sparkCtx.getConf.set("spark.executor.instances", s"${excnt-1}")
    val dmode = sparkCtx.deployMode

    if(remoteSpark || !localSpark){ 
      val credentialName = new File(sheetCred).getName
      val hdfsHome = HadoopUtils.getHomeDirectoryHDFS(sparkCtx)
      val hdfsPath = if(remoteSpark) s"$hdfsHome/" else ""
      val overwriteJars = false//config.overwriteJars()
      //sparkCtx.hadoopConfiguration.set("spark.sql.warehouse.dir",s"${hdfsPath}metastore_db")
      //sparkCtx.hadoopConfiguration.set("hive.metastore.warehouse.dir",s"${hdfsPath}metastore_db")
      HadoopUtils.writeToHDFS(sparkCtx, s"mimir-core_${scalaVersion}-0.3.jar", new File(getJarPath("info.mimirdb", "mimir-core", "0.3", scalaVersion)), overwriteJars)
      HadoopUtils.writeToHDFS(sparkCtx, s"scala-logging_${scalaVersion}-3.9.0.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/com.typesafe.scala-logging/scala-logging_${scalaVersion}/bundles/scala-logging_${scalaVersion}-3.9.0.jar"), overwriteJars)
      HadoopUtils.writeToHDFS(sparkCtx, s"play-json_${scalaVersion}-2.7.0-M1.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/com.typesafe.play/play-json_${scalaVersion}/jars/play-json_${scalaVersion}-2.7.0-M1.jar"), overwriteJars)
      HadoopUtils.writeToHDFS(sparkCtx, s"play-functional_${scalaVersion}-2.7.0-M1.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/com.typesafe.play/play-functional_${scalaVersion}/jars/play-functional_${scalaVersion}-2.7.0-M1.jar"), overwriteJars)
      HadoopUtils.writeToHDFS(sparkCtx, s"jsr-275-0.9.1.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/javax.measure/jsr-275/jars/jsr-275-0.9.1.jar"), overwriteJars)
      HadoopUtils.writeToHDFS(sparkCtx, s"postgresql-9.4-1201-jdbc41.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/org.postgresql/postgresql/jars/postgresql-9.4-1201-jdbc41.jar"), overwriteJars)
      HadoopUtils.writeToHDFS(sparkCtx, s"sqlite-jdbc-3.16.1.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/org.xerial/sqlite-jdbc/jars/sqlite-jdbc-3.16.1.jar"), overwriteJars)
      HadoopUtils.writeToHDFS(sparkCtx, s"spark-xml_${scalaVersion}-0.5.0.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/com.databricks/spark-xml_${scalaVersion}/jars/spark-xml_${scalaVersion}-0.5.0.jar"), overwriteJars)
      HadoopUtils.writeToHDFS(sparkCtx, s"spark-excel_${scalaVersion}-0.12.0.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/com.crealytics/spark-excel_${scalaVersion}/jars/spark-excel_${scalaVersion}-0.12.0.jar"), overwriteJars)
      HadoopUtils.writeToHDFS(sparkCtx, s"google-api-services-sheets-v4-rev18-1.22.0.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/com.google.apis/google-api-services-sheets/jars/google-api-services-sheets-v4-rev18-1.22.0.jar"), overwriteJars)
      HadoopUtils.writeToHDFS(sparkCtx, s"google-api-client-1.22.0.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/com.google.api-client/google-api-client/jars/google-api-client-1.22.0.jar"), overwriteJars)
      HadoopUtils.writeToHDFS(sparkCtx, s"google-api-client-json-1.2.2-alpha.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/com.google.api.client/google-api-client-json/jars/google-api-client-json-1.2.2-alpha.jar"), overwriteJars)
      HadoopUtils.writeToHDFS(sparkCtx, s"google-api-client-util-1.2.2-alpha.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/com.google.api.client/google-api-client-util/jars/google-api-client-util-1.2.2-alpha.jar"), overwriteJars)
      HadoopUtils.writeToHDFS(sparkCtx, s"sparsity_${scalaVersion}-${sparsityVersion}.jar", new File(getJarPath("info.mimirdb", "sparsity", sparsityVersion, scalaVersion)), overwriteJars)
      HadoopUtils.writeToHDFS(sparkCtx, s"fastparse_${scalaVersion}-2.1.0.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/com.lihaoyi/fastparse_${scalaVersion}/jars/fastparse_${scalaVersion}-2.1.0.jar"), overwriteJars)
      HadoopUtils.writeToHDFS(sparkCtx, s"$credentialName",new File(s"test/data/$credentialName"), overwriteJars)
      //HadoopUtils.writeToHDFS(sparkCtx, "aws-java-sdk-s3-1.11.355.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/com.amazonaws/aws-java-sdk-s3/jars/aws-java-sdk-s3-1.11.355.jar"), overwriteJars)
      //HadoopUtils.writeToHDFS(sparkCtx, "hadoop-aws-2.7.6.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/org.apache.hadoop/hadoop-aws/jars/hadoop-aws-2.7.6.jar"), overwriteJars)
      HadoopUtils.writeToHDFS(sparkCtx, "geospark-1.2.0.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/org.datasyslab/geospark/jars/geospark-1.2.0.jar"), overwriteJars)
      HadoopUtils.writeToHDFS(sparkCtx, "geospark-sql_2.3-1.2.0.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/org.datasyslab/geospark-sql_2.3/jars/geospark-sql_2.3-1.2.0.jar"), overwriteJars)
      HadoopUtils.writeToHDFS(sparkCtx, s"scallop_${scalaVersion}-3.1.3.jar", new File(s"${System.getProperty("user.home")}/.ivy2/cache/org.rogach/scallop_${scalaVersion}/jars/scallop_${scalaVersion}-3.1.3.jar"), overwriteJars)
      
      sparkCtx.addJar(s"${hdfsPath}mimir-core_${scalaVersion}-0.3.jar")
      sparkCtx.addJar(s"${hdfsPath}scala-logging_${scalaVersion}-3.9.0.jar")       
      sparkCtx.addJar(s"${hdfsPath}play-json_${scalaVersion}-2.7.0-M1.jar")  
      sparkCtx.addJar(s"${hdfsPath}play-functional_${scalaVersion}-2.7.0-M1.jar")  
      sparkCtx.addJar(s"${hdfsPath}jsr-275-0.9.1.jar")                                     
      sparkCtx.addJar(s"${hdfsPath}postgresql-9.4-1201-jdbc41.jar")
      sparkCtx.addJar(s"${hdfsPath}sqlite-jdbc-3.16.1.jar")
      sparkCtx.addJar(s"${hdfsPath}spark-xml_${scalaVersion}-0.5.0.jar")
      sparkCtx.addJar(s"${hdfsPath}spark-excel_${scalaVersion}-0.12.0.jar")
      sparkCtx.addJar(s"${hdfsPath}google-api-services-sheets-v4-rev18-1.22.0.jar")
      sparkCtx.addJar(s"${hdfsPath}google-api-client-1.22.0.jar")
      sparkCtx.addJar(s"${hdfsPath}google-api-client-json-1.2.2-alpha.jar")
      sparkCtx.addJar(s"${hdfsPath}google-api-client-util-1.2.2-alpha.jar")
      sparkCtx.addJar(s"${hdfsPath}sparsity_${scalaVersion}-${sparsityVersion}.jar")
      sparkCtx.addJar(s"${hdfsPath}fastparse_${scalaVersion}-2.1.0.jar")
      sparkCtx.addFile(s"${hdfsPath}$credentialName")
      sparkCtx.addJar(s"${hdfsPath}geospark-1.2.0.jar")
      sparkCtx.addJar(s"${hdfsPath}geospark-sql_2.3-1.2.0.jar")
      sparkCtx.addJar(s"${hdfsPath}scallop_${scalaVersion}-3.1.3.jar")
      
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

    sparkSql = sparkSession.sqlContext//new SQLContext(sparkCtx)
    GeoSparkSQLRegistrator.registerAll(sparkSession)  
  }
  
  def getJarPath(repoPath:String, libName:String, libVersion:String, scalaVersion:String):String = {
    val libIvyPath = s"${System.getProperty("user.home")}/.ivy2/cache/${repoPath}/${libName}_${scalaVersion}/jars/${libName}_${scalaVersion}-${libVersion}.jar"
    val libM2Path = s"${System.getProperty("user.home")}/.m2/repository/${repoPath.replaceAll("\\.", "/")}/${libName}_${scalaVersion}/${libVersion}/${libName}_${scalaVersion}-${libVersion}.jar"
    if(new File(libIvyPath).exists() ) libIvyPath else libM2Path
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
          val fInfo = get.sparkSession.sessionState.catalog.lookupFunctionInfo(fidentifier)
          val isGeosparkFunction = fidentifier.toString().startsWith("st_")
          if(fInfo != null){
            val fClassName = fInfo.getClassName
            if(fClassName != null && !fClassName.startsWith("org.apache.spark.sql.catalyst.expressions.aggregate")){
              logger.debug("registering spark function: " + fidentifier.funcName)
              SparkFunctions.addSparkFunction(ID(fidentifier.funcName), (inputs) => {
                val sparkInputs = inputs.map(inp => 
                  if(isGeosparkFunction){
                    Literal(RAToSpark.mimirPrimitiveToSparkExternalInlineFuncParamGeo(inp))
                  }
                  else{
                    Literal(RAToSpark.mimirPrimitiveToSparkExternalInlineFuncParam(inp))
                  })
                val sparkInternal = inputs.map(inp => 
                  if(isGeosparkFunction){
                    RAToSpark.mimirPrimitiveToSparkInternalInlineFuncParamGeo(inp)
                  }
                  else {
                    RAToSpark.mimirPrimitiveToSparkInternalInlineFuncParam(inp)
                  })
                val sparkRow = InternalRow(sparkInternal:_*)
                val sparkFunc = if(isGeosparkFunction){
                  val constructors = Class.forName(fClassName.replaceAll("\\$", "")).getDeclaredConstructors
                  val constructor = constructors.head
                  constructor.setAccessible(true)
                  constructor.newInstance(sparkInputs)
                  .asInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression]
                }
                else{
                  val constructorTypes = inputs.map(inp => classOf[org.apache.spark.sql.catalyst.expressions.Expression])
                  Class.forName(fClassName).getDeclaredConstructor(constructorTypes:_*).newInstance(sparkInputs:_*)
                                  .asInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression]
                }
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
                if(isGeosparkFunction){
                  val inputs = inputTypes.map(inp => Literal(RAToSpark.getNativeGeo(NullPrimitive(), inp)).asInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression])
                  val constructors = Class.forName(fClassName.replaceAll("\\$", "")).getDeclaredConstructors
                  val constructor = constructors.head
                  constructor.setAccessible(true)
                  RAToSpark.getMimirType( constructor.newInstance(inputs)
                  .asInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression].dataType)
                }
                else{
                  val inputs = inputTypes.map(inp => Literal(RAToSpark.getNative(NullPrimitive(), inp)).asInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression])
                  val constructorTypes = inputs.map(inp => classOf[org.apache.spark.sql.catalyst.expressions.Expression])
                  RAToSpark.getMimirType( Class.forName(fClassName).getDeclaredConstructor(constructorTypes:_*).newInstance(inputs:_*)
                  .asInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression].dataType)
                }
              })
            } 
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
          if(fClassName != null && fClassName.startsWith("org.apache.spark.sql.catalyst.expressions.aggregate")){
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
  
  def isSparkRunning():Boolean = {
    /*val sparkMasterProcess = Process(
    Seq("ls", "/tmp/"),
    cwd = new File("."))
    val sparkPidRegex = "spark\\-.*\\.pid".r
    sparkMasterProcess.!!.split("\n").map( lsr => lsr match {
      case x@sparkPidRegex() => true
      case x => false
    }).fold(false)((init, curr) => init || curr)*/
    val sparkProcesses = Process(
    Seq("ps", "-a", "-o", "args"),
    cwd = new File("."))
    val sparkMasterRegex = """.*org\.apache\.spark\.deploy\.master\.Master.*""".r
    val sparkWorkerRegex = """.*org\.apache\.spark\.deploy\.worker\.Worker.*""".r
    sparkProcesses.!!.split("\n").map( lsr => lsr match {
      case x@sparkMasterRegex() => 0x1
      case x@sparkWorkerRegex() => 0x2
      case x => 0x0
    }).fold(0x0)((init, curr) => init | curr) == 0x3
    
  }
  
  def installAndRunSpark(config: MimirConfig):Unit = {
    val dataDir = if(config.dataDirectory().endsWith("/")) config.dataDirectory() else config.dataDirectory() + "/"
    val sparkDir = s"${dataDir}spark"
    val sparkDirF = new File(sparkDir)
    println(s"data directory: $dataDir")
    if(isSparkRunning()) {
      println("spark is already running-------------------------------------")
      return
    }
    println("running spark-------------------------------------")
    val sparkVersion = "spark-2.4.4-bin-without-hadoop-scala-2.12"//"spark-2.4.4-bin-hadoop2.7"
    val dataDirF = new File(dataDir)
    val sparkVerDirF = new File(s"${sparkDir}/${sparkVersion}")
    if(!sparkVerDirF.exists()){
      sparkDirF.mkdirs()
      //dist url would be: s"https://www-us.apache.org/dist/spark/spark-2.4.4/${sparkVersion}.tgz"
      val sparkTarStream = new URL(s"https://vizierdb.info/${sparkVersion}.tgz").openStream();
      FileUtils.untar(sparkTarStream, sparkDir)
      //new URL("https://www-us.apache.org/dist/spark/spark-2.4.4/${sparkVersion}.tgz") #> new File(s"$sparkDir/${sparkVersion}.tgz") !!
      //FileUtils.untar(new FileInputStream(s"$sparkDir/${sparkVersion}.tgz"), sparkDir)
      val sbinFiles = listOfFiles(s"$sparkDir/${sparkVersion}/sbin")
      sbinFiles.map(_.setExecutable(true))
      val binFiles = listOfFiles(s"$sparkDir/${sparkVersion}/bin")
      binFiles.map(_.setExecutable(true))
    }
    val localIpAddress: String = InetAddress.getLocalHost.getHostAddress
    val sparkMasterProcess = Process(
      s"$sparkDir/${sparkVersion}/sbin/start-master.sh",
      cwd = dataDirF,
      extraEnv = ("SPARK_MASTER_HOST", localIpAddress), ("SPARK_MASTER_PORT", s"${config.sparkPort()}")).!
      
    val sparkSlaveProcess = Process(
      Seq(s"$sparkDir/${sparkVersion}/sbin/start-slave.sh", s"$localIpAddress:${config.sparkPort()}"),
      cwd = dataDirF,
      extraEnv = ("SPARK_MASTER_HOST", localIpAddress), ("SPARK_WORKER_INSTANCES", "3")).!
     
  }
  private def listOfFiles(path : String) : List[File] = {
        val paths = Paths.get(path)
        val file = paths.toFile
        if(file.isDirectory){
            file.listFiles().toList
        }else List[File]() 
    }
}
