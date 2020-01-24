package mimir;

import java.io._
import java.util.Vector
import java.util.UUID
import java.net.InetAddress
import java.net.URLDecoder

import scala.collection.convert.Wrappers.JMapWrapper
import scala.tools.reflect.ToolBox
import scala.reflect.runtime.currentMirror
import play.api.libs.json._

import org.rogach.scallop._
import org.slf4j.{LoggerFactory}
import ch.qos.logback.classic.{Level, Logger}
import com.typesafe.scalalogging.slf4j.LazyLogging

import sparsity.Name
import sparsity.statement.CreateView
import sparsity.parser.{SQL,Expression} 
import fastparse.Parsed

import org.apache.spark.sql.SparkSession

import mimir.algebra._
import mimir.algebra.function.SparkFunctions
import mimir.api.MimirAPI
import mimir.api.{CodeEvalResponse, CreateLensResponse, DataContainer, Schema}
import mimir.ctables.AnalyzeUncertainty
import mimir.ctables.Reason
import mimir.data.staging.{ RawFileProvider, LocalFSRawFileProvider }
import mimir.exec.Compiler
import mimir.exec.result.{ ResultIterator, Row }
import mimir.exec.mode.{ UnannotatedBestGuess }
import mimir.exec.spark.{MimirSpark, MimirSparkRuntimeUtils}
import mimir.metadata.JDBCMetadataBackend
import mimir.ml.spark.SparkML
import mimir.parser._
import mimir.parser.ExpressionParser
import mimir.serialization.AlgebraJson
import mimir.data.staging.HDFSRawFileProvider
import mimir.util.{
  JSONBuilder,
  LoggerUtils,
  ExperimentalOptions,
  Timer
}
import java.sql.SQLException
import mimir.ctables.CoarseDependency

/**
 * The interface to Mimir for Vistrails.  Responsible for:
 * - Parsing and processing command line arguments.
 * - Initializing internal state (Database())
 * - Providing a Gateway Server to allow python 
 *   to make calls here
 * - Invoking MimirJSqlParser and dispatching the 
 *   resulting statements to Database()
 *
 * Database() handles all of the logical dispatching,
 * MimirVizier provides a py4j gateway server 
 * interface on top of Database()
 */
object MimirVizier extends LazyLogging {

  val VIZIER_DATA_PATH = Option(System.getenv("VIZIER_DATA_PATH")) match {
    case Some(vizierDataPath) => vizierDataPath
    case _ => "/usr/local/source/web-api/vizier/.vizierdb/"
  }
  var db: Database = null;
  var usePrompt = true;
  
  def main(args: Array[String]) {
    val conf = new MimirConfig(args);
    ExperimentalOptions.enable(conf.experimental())

    // For Vizier specifically, we want Derby/Hive support enabled
    ExperimentalOptions.enable("USE-DERBY")
    
    val dataDir = new java.io.File(conf.dataDirectory())
    if(!dataDir.exists())
      dataDir.mkdirs()
   
    
    // Set up the database connection(s)
    val database = conf.dbname().split("[\\\\/]").last.replaceAll("\\..*", "")
    MimirSpark.init(conf)
    val metadata = new JDBCMetadataBackend(conf.metadataBackend(), conf.dbname())
    val staging = if(conf.dataStagingType().equalsIgnoreCase("hdfs") && ExperimentalOptions.isEnabled("remoteSpark"))
      new HDFSRawFileProvider()
    else 
      new LocalFSRawFileProvider(new java.io.File(conf.dataDirectory()))

    db = new Database(metadata, staging)
    VizierDB.sparkSession = MimirSpark.get.sparkSession
    
   if(ExperimentalOptions.isEnabled("WEB-LOG")){
      LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME) match {
          case logger: Logger => {
            val hookUrl = System.getenv("LOG_HOOK_URL")
            val token = System.getenv("LOG_HOOK_TOKEN")
            logger.addAppender(new mimir.util.WebLogAppender(hookUrl,token)) 
          }
        }
    }
    
    if(ExperimentalOptions.isEnabled("LOG")){
      val logLevel = 
        if(ExperimentalOptions.isEnabled("LOGD")) Level.DEBUG
        else if(ExperimentalOptions.isEnabled("LOGW")) Level.WARN
        else if(ExperimentalOptions.isEnabled("LOGE")) Level.ERROR
        else if(ExperimentalOptions.isEnabled("LOGI")) Level.INFO
        else if(ExperimentalOptions.isEnabled("LOGO")) Level.OFF
        else Level.DEBUG
       
      val mimirVizierLoggers = Seq("mimir.exec.spark.RAToSpark", "mimir.exec.spark.MimirSpark", this.getClass.getName, 
          "mimir.api.MimirAPI", "mimir.api.MimirVizierServlet", "mimir.data.staging.HDFSRawFileProvider")
      mimirVizierLoggers.map( mvLogger => {
        LoggerFactory.getLogger(mvLogger) match {
          case logger: Logger => {
            logger.setLevel(logLevel)
            logger.debug(s"$mvLogger logger set to level: " + logLevel); 
          }
        }
      })
      
      LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME) match {
          case logger : Logger if(!ExperimentalOptions.isEnabled("LOGM")) => {
            logger.setLevel(logLevel)
            logger.debug("root logger set to level: " + logLevel); 
          }
          case _ => logger.debug("logging settings from logback.xml");
        }
    }
    
    
    if(!ExperimentalOptions.isEnabled("NO-VISTRAILS")){
      MimirAPI.runAPIServerForViztrails()
      db.close()
      if(!conf.quiet()) { logger.debug("\n\nDone.  Exiting."); }
    }
    
    
  }
 

  object Eval {
  
    def apply[A](string: String): A = {
      val toolbox = currentMirror.mkToolBox()
      val tree = toolbox.parse(string)
      toolbox.eval(tree).asInstanceOf[A]
    }
  
    def fromFile[A](file: File): A =
      apply(scala.io.Source.fromFile(file).mkString(""))
  
    def fromFileName[A](file: String): A =
      fromFile(new File(file))
  
  }
  
  object VizierDB {
    
    var sparkSession:SparkSession = null;
    
    def withDataset[T](dsname:String, handler: ResultIterator => T ) : T = {
      val oper = db.tempViews.get(ID(dsname)) match {
        case Some(viewQuery) => viewQuery
        case None => throw new Exception(s"No such table or view '$dsname'")
      }
      db.query(oper)(handler)
    }
        
    def outputAnnotations(dsname:String) : String = {
      val oper = db.tempViews.get(ID(dsname)) match {
        case Some(viewQuery) => viewQuery
        case None => throw new Exception(s"No such table or view '$dsname'")
      }
      explainEverything(oper).mkString("<br><br>")
    }
  }
  
  //-------------------------------------------------
  //Mimir API impls
  ///////////////////////////////////////////////
  var apiCallThread : Thread = null
  def evalScala(inputs:Map[String, String], source : String) : CodeEvalResponse = {
    try {
      val timeRes = logTime("evalScala") {
        registerNameMappings(inputs)
        Eval("import mimir.MimirVizier.VizierDB\n"+source) : String
      }
      logger.debug(s"evalScala Took: ${timeRes._2}")
      CodeEvalResponse(timeRes._1,"")
    } catch {
      case t: Throwable => {
        logger.error(s"Error Evaluating Scala Source", t)
        CodeEvalResponse("",s"Error Evaluating Scala Source: \n${t.getMessage()}\n${t.getStackTrace.mkString("\n")}\n${t.getMessage()}\n${t.getCause.getStackTrace.mkString("\n")}")
      }
    }
  }
  
  
  def evalR(source : String) : CodeEvalResponse = {
    try {
      val timeRes = logTime("evalR") {
        val datasetMatch = """vizierdb\$getDataset\(\w*"([a-zA-Z0-9_-]+)"\w*\)""".r 
        val (tmpNames, dsNames) = source.
        split("\n").toSeq
        .zipWithIndex
        .flatMap(line => 
          line._1 match {
            case datasetMatch(dsName) => {
              val tmpName = s"temp_view_${source.hashCode()}_${line._2}"
              db.compiler.compileToSparkWithRewrites(db.table(dsName))
                .createOrReplaceTempView(tmpName)
                Some((tmpName, dsName))
              }
            case x => None
        }).unzip
        val rLibCode = s"""
        print("mimir lib code")
        """ + source
        val R = org.ddahl.rscala.RClient("R",0,true)
        val ret = R.evalS0(rLibCode)
        R.quit()
        ret
      }
      logger.debug(s"evalScala Took: ${timeRes._2}")
      CodeEvalResponse(timeRes._1,"")
    } catch {
      case t: Throwable => {
        logger.error(s"Error Evaluating R Source", t)
        CodeEvalResponse("",s"Error Evaluating Scala Source: \n${t.getMessage()}\n${t.getStackTrace.mkString("\n")}\n${t.getMessage()}\n${t.getCause.getStackTrace.mkString("\n")}")
      }
    }
  }
  
  def setSparkWorkers(count:Int):Unit = {
    MimirSpark.get.setConf("spark.sql.shuffle.partitions", count.toString())
  }
  
  def executeOnWorkers(oper:Operator, col:String):Unit = {
    val (compiledOp, cols, metadata) = mimir.exec.mode.BestGuess.rewrite(db, oper)
    val colidx = cols.indexOf(col)
    val dfRowFunc: (Iterator[org.apache.spark.sql.Row]) => Unit = (rows) => {
      rows.map(row => row.get(colidx))                                     
    }                                  
    db.compiler.executeOnWorkers(compiledOp, dfRowFunc)                                                                                             
  }      
  
  def executeOnWorkersReturn(oper:Operator, col:String):Int = {
    import org.apache.spark.sql.functions.sum
    val (compiledOp, cols, metadata) = mimir.exec.mode.BestGuess.rewrite(db, oper)
    val colidx = cols.indexOf(col)
    val df = db.compiler.compileToSparkWithoutRewrites(compiledOp)
    val mapFunc: (Iterator[org.apache.spark.sql.Row]) => Iterator[Int] = (rows) => {
      rows.map(row => {
          row.get(colidx)
          1
        }).toSeq.iterator
    } 
    val newDF = df.mapPartitions(mapFunc)(org.apache.spark.sql.Encoders.scalaInt).toDF()
    newDF.agg(sum(newDF.columns(0))).collect.toSeq.head.getInt(0)
  }  
 
  
  def loadCSV(file : String) : String = loadDataSource(file, "csv", Seq(("delimeter",",")))
  def loadCSV(file : String, delimeter:String, detectHeaders:Boolean, inferTypes:Boolean) : String = 
    loadDataSource(file, "csv", Seq(("delimeter",delimeter),("mimir_detect_headers",detectHeaders.toString()),("mimir_infer_types",inferTypes.toString())))
  def loadDataSource(file : String, format:String, options:Seq[(String, String)]) : String = {
    val detectHeaders     = options.filter { _._1.equalsIgnoreCase("mimir_detect_headers") }
                                   .headOption
                                   .map { _._2.equalsIgnoreCase("false") }
                                   .getOrElse(true)
    val inferTypes        = options.filter { _._1.equalsIgnoreCase("mimir_infer_types") }
                                   .headOption
                                   .map { _._2.equalsIgnoreCase("false") }
                                   .getOrElse(true)
    val humanReadableName = options.filter { _._1.equalsIgnoreCase("mimir_human_readable_name") }
                                   .headOption
                                   .map { _._2 }

    val backendOptions = 
      options.filterNot { x => 
        x._1.equalsIgnoreCase("mimir_detect_headers") || 
        x._1.equalsIgnoreCase("mimir_infer_types") || 
        x._1.equalsIgnoreCase("mimir_human_readable_name")
      }
    loadDataSource(file, format, inferTypes, detectHeaders, humanReadableName, backendOptions)
  }
  def loadDataSource(file : String, format:String, inferTypes:Boolean, detectHeaders:Boolean, humanReadableName:Option[String], backendOptions:Seq[Any], dependencies:Seq[String]=Seq()) : String = {
    try{
      apiCallThread = Thread.currentThread()
      val timeRes = logTime("loadDataSource") {
      logger.debug(s"loadDataSource: From Vistrails: [ $file ] inferTypes: $inferTypes detectHeaders: $detectHeaders format: ${format} -> [ ${backendOptions.mkString(",")} ]") ;
      val bkOpts = backendOptions.map{
        case (optKey:String, optVal:String) => (optKey, optVal)
        case hm:java.util.HashMap[_,_] => {
          val entry = hm.asInstanceOf[java.util.HashMap[String,String]].entrySet().iterator().next()
          (entry.getKey, entry.getValue)
        }
        case _ => throw new Exception("loadDataSource: bad options type")
      }
      
      val saferFile = URLDecoder.decode(file, "utf-8")
      val useS3Volume = System.getenv("USE_S3_VOLUME") match {
        case null => false
        case "true" => true
        case x => false
      }
      val csvFile = if(saferFile.startsWith(VIZIER_DATA_PATH) && useS3Volume){
        //hack for loading file from s3 - because it is already there for production version
        val vizierDataS3Bucket = System.getenv("S3_BUCKET_NAME")
        saferFile.replace(VIZIER_DATA_PATH, s"s3n://$vizierDataS3Bucket/")
      }
      else{
        saferFile
      }
      val fileName = new File(csvFile).getName().split("\\.")(0)
      val tableName = sanitizeTableName(fileName)
      if(db.catalog.tableExists(tableName)){
        logger.debug("loadDataSource: From Vistrails: Table Already Exists: " + tableName)
      }
      else{
        val loadOptions = bkOpts.toMap
        db.loader.loadTable(
          sourceFile = csvFile,
          targetTable = Some(tableName), 
          // targetSchema = None, 
          inferTypes = Some(inferTypes), 
          detectHeaders = Some(detectHeaders), 
          sparkOptions = loadOptions, 
          format = ID(format),
          humanReadableName = humanReadableName,
          datasourceErrors = loadOptions.getOrElse("datasourceErrors", "false").equals("true"),
          stageSourceURL = true
        )
      }
      val (realTargetSchema, realTarget, _) = 
            db.catalog.resolveTable(tableName)
                   .getOrElse { throw new SQLException(s"Unknown target table ${tableName}")}
      val targetTable = (realTargetSchema, realTarget)
      dependencies.foreach(dependency => {
        val (realSourceSchema, realSource, _) = 
              db.catalog.resolveTableCaseInsensitive(dependency)
                     .getOrElse { throw new SQLException(s"Unknown dependent table ${dependency}") }
        db.catalog.createDependency(targetTable, CoarseDependency(realSourceSchema, realSource))
      })
      tableName 
    }
    logger.debug(s"loadDataSource ${timeRes._1.toString} Took: ${timeRes._2}")
    timeRes._1.toString
    } catch {
      case t: Throwable => {
        logger.error(s"Error Loading Data: $file", t)
        throw t
      }
    }
  }
  
  def unloadDataSource(input:String, file : String, format:String, backendOptions:Seq[Any]) : List[String] = {
    try{
      val timeRes = logTime("loadDataSource") {
        logger.debug("unloadDataSource: From Vistrails: [" + input + "] [" + file + "] [" + format + "] [ " + backendOptions.mkString(",") + " ]"  ) ;
        val bkOpts = backendOptions.map{
          case (optKey:String, optVal:String) => (optKey, optVal)
          case hm:java.util.HashMap[_,_] => {
            val entry = hm.asInstanceOf[java.util.HashMap[String,String]].entrySet().iterator().next()
            (entry.getKey, entry.getValue)
          }
          case _ => throw new Exception("unloadDataSource: bad options type")
        }
        val df = db.compiler.compileToSparkWithRewrites(
            db.catalog.tableOperator(Name(input))
          )

        MimirSparkRuntimeUtils.writeDataSink(
            df, 
            format, 
            bkOpts.toMap, 
            if(file == null || file.isEmpty()) None else Some(file)
          )
          if(!(file == null || file.isEmpty())){
            val filedir = new File(file)
            filedir.listFiles.filter(_.isFile)
              .map(_.getName).toList
          }
          else List[String]()
      }
      logger.debug(s"unloadDataSource Took: ${timeRes._2}")
      timeRes._1
    } catch {
      case t: Throwable => {
        logger.error(s"Error Unloading Data: $file", t)
        throw t
      }
    }
  }
  
  private def sanitizeTableName(tableName:String) : ID = {
    //table names cant start with digits - the sql parser does not like it
    //to if the filename starts with a digit, prepend a "t"
    ID.upper(
      ((if(tableName.matches("^\\d.*")) s"t$tableName" else tableName) + UUID.randomUUID().toString())
        //also replace characters that cant be in table name with _
        .replaceAll("[\\%\\^\\&\\(\\{\\}\\+\\-\\/ \\]\\[\\'\\?\\=\\!\\`\\~\\\\\\/\\\"\\:\\;\\<\\>\\.\\,\\*\\$\\#\\@]", "_") 
    )
  }
  
  def createLens(
    input : Any, 
    params : JsValue, 
    _type : String, 
    materialize:Boolean, 
    humanReadableName: Option[String]
  ) : CreateLensResponse = {
    createLens(
      input              = input, 
      params             = params,
      _type              = _type, 
      materialize        = materialize,
      humanReadableName  = humanReadableName
    )
  }
  
  def createLens(
    input : Any, 
    params : String, 
    _type : String, 
    materialize:Boolean, 
    humanReadableName: Option[String]
  ) : CreateLensResponse = {
    try{
    apiCallThread = Thread.currentThread()
    val timeRes = logTime("createLens") {
      logger.debug("createLens: From Vistrails: [" + input + "] [" + 
                    params.mkString(",") + "] [" + _type + "]"  ) ;
      
      val parsedParams = Json.parse(params)
      val lensNameBase = (input.toString() + _type + params + materialize).hashCode()
      val inputQuery = s"SELECT * FROM ${input}"

      val lensName = "LENS_" + _type + (lensNameBase.toString().replace("-", ""))
      val lensType = _type.toUpperCase()

      if(ExperimentalOptions.isEnabled("FORCE-LENS-REBUILD") && db.catalog.tableExists(Name(lensName))) {
        db.lenses.drop(ID(lensName))
      }
      if(db.catalog.tableExists(Name(lensName))){
        logger.debug("createLens: From Vistrails: Lens (or Table) already exists: " + 
                     lensName)
        // (Should be) safe to fall through since we might still be getting asked to materialize 
        // the table.
        db.lenses.drop(ID(lensName))
      } else {
        // Need to create the lens if it doesn't already exist.
        // query is a var because we might need to rewrite it below.
        var query:Operator = db.catalog.tableOperator(Name(input.toString))
        // Regardless of where we're reading from, next we need to run: 
        //   CREATE LENS ${lensName} 
        //            AS $query 
        //          WITH ${_type}( ${params.mkString(",")} )
        // Skip the parser and do what Mimir does internally
        db.lenses.create(
          ID(lensType), 
          ID(lensName),
          query, 
          parsedParams, 
          // Vizier uses funky custom table names internally.
          // Use the source table as a name for human-visible 
          // outputs like uncertainty explanations.
          friendlyName = humanReadableName
        )
      }
      if(materialize){
        if(!db.views(ID(lensName)).isMaterialized){
          db.views.materialize(ID(lensName))
        }
      }
      val lensOp = db.catalog.tableOperator(Name(lensName)).limit(200)
      //val lensAnnotations = db.explainer.explainSubsetWithoutOptimizing(lensOp, lensOp.columnNames.toSet, false, false, false, Some(ID(lensName)))
      val lensReasons = 0//lensAnnotations.map(_.size(db)).sum//.toString//JSONBuilder.list(lensAnnotations.map(_.all(db).toList).flatten.map(_.toJSON))
      logger.debug(s"createLens reasons for first 200 rows: ${lensReasons}")
      CreateLensResponse(lensName.toString, lensReasons)
    }
    logger.debug(s"createLens ${_type} Took: ${timeRes._2}")
    timeRes._1
    } catch {
      case t: Throwable => {
        logger.error(s"Error Creating Lens: [ $input ] [ ${params.mkString(",")} ] [ ${_type }]", t)
        throw t
      }
    }
  }
  
  def registerNameMappings(nameMappings:JMapWrapper[String,String]) : Unit = {
    try{
      nameMappings.map{ case (vizierName, mimirName) => registerNameMapping(vizierName, mimirName) }
     } catch {
      case t: Throwable => {
        logger.error("Failed To Register Name Mappings: [" + nameMappings.mkString(",") + "]", t)
        throw t
      }
    }
  }
  
  private def registerNameMappings(nameMappings:Map[String,String]) : Unit = {
    try{
      nameMappings.map{ case (vizierName, mimirName) => registerNameMapping(vizierName, mimirName) }
     } catch {
      case t: Throwable => {
        logger.error("Failed To Register Name Mappings: [" + nameMappings.mkString(",") + "]", t)
        throw t
      }
    }
  }
  
  private def registerNameMapping(vizierName:String, mimirName:String) : Unit = {
    db.tempViews.put(
      ID(vizierName), 
      db.catalog.tableOperator(Name(mimirName))
    )
  }
  
  def createView(input : Any, query : String) : String = {
    try{
    apiCallThread = Thread.currentThread()
    val timeRes = logTime("createLens") {
      logger.debug("createView: From Vistrails: [" + input + "] [" + query + "]"  ) ;
      val (viewNameSuffix, inputSubstitutionQuery) = input match {
        case aliases:Map[_,_] => {
          registerNameMappings(aliases.asInstanceOf[Map[String,String]]) 
          (aliases.toSeq.unzip._2.mkString(""), query)
        }
        case aliases:JMapWrapper[_,_] => {
          registerNameMappings(aliases.asInstanceOf[Map[String,String]]) 
          (aliases.asInstanceOf[JMapWrapper[String,String]].unzip._2.mkString(""), query)
        }
        case inputs:Seq[_] => {
          (inputs.asInstanceOf[Seq[String]].mkString(""),inputs.asInstanceOf[Seq[String]].zipWithIndex.foldLeft(query)((init, curr) => {
            init.replaceAll(s"\\{\\{\\s*input_${curr._2}\\s*\\}\\}", curr._1) 
          })) 
        }
        case _:String => {
          (input.toString(), query.replaceAll("\\{\\{\\s*input[_0]*\\s*\\}\\}", input.toString)) 
        }
        case x => throw new Exception(s"Parameter type ${x.getClass()} is invalid for createView input" )
      }
      
      val viewName = "VIEW_" + ((viewNameSuffix + query).hashCode().toString().replace("-", "") )
      // db.getView(viewName) match {
      if(db.catalog.tableExists(Name(viewName))){
        logger.warn("createView: From Vistrails: View already exists: " + viewName)
      } else {
        val viewQuery = SQL(inputSubstitutionQuery) match {
          case fastparse.Parsed.Success(sparsity.statement.Select(body, _), _) => body
          case x => throw new Exception(s"Invalid view query : $inputSubstitutionQuery \n $x")
        }
        logger.debug("createView: query: " + viewQuery)
        db.update(SQLStatement(CreateView(Name(viewName, true), false, viewQuery)))
      }
      viewName
    }
    logger.debug(s"createView ${timeRes._1.toString} Took: ${timeRes._2}")
    timeRes._1.toString
    } catch {
      case t: Throwable => {
        logger.error("Error Creating View: [" + input + "] [" + query + "]", t)
        throw t
      }
    }
  }
  
  def createAdaptiveSchema(input : Any, params : JsValue, _type : String) : String = {
    val paramsStr = params.toString()
    try {
      apiCallThread = Thread.currentThread()
      val timeRes = logTime("createAdaptiveSchema") {
      logger.debug("createAdaptiveSchema: From Vistrails: [" + input + "] [" + paramsStr + "]"  ) ;
      val adaptiveSchemaName = ID("ADAPTIVE_SCHEMA_" + _type + ((input.toString() + _type + paramsStr).hashCode().toString().replace("-", "") ))
      val asViewName = ID("VIEW_"+adaptiveSchemaName)
      if(db.catalog.tableExists(adaptiveSchemaName)){
        logger.debug("createAdaptiveSchema: From Vistrails: Adaptive Schema already exists: " + adaptiveSchemaName)
      } else {
        db.lenses.create(
          ID(_type), 
          adaptiveSchemaName, 
          db.catalog.tableOperator(Name(input.toString)), 
          params, 
          Some(input.toString)
        )
      }
      asViewName
    }
    logger.debug(s"createView ${timeRes._1.toString} Took: ${timeRes._2}")
    timeRes._1.toString
    } catch {
      case t: Throwable => {
        logger.error("Error Creating Adaptive Schema: [" + input + "] [" + paramsStr + "]", t)
        throw t
      }
    }
  }
  
  def vistrailsQueryMimir(input:Any, query : String, includeUncertainty:Boolean, includeReasons:Boolean) : DataContainer = {
    val inputSubstitutionQuery = query.replaceAll("\\{\\{\\s*input\\s*\\}\\}", input.toString) 
    vistrailsQueryMimir(inputSubstitutionQuery, includeUncertainty, includeReasons)
  }
  
  def vistrailsQueryMimir(query : String, includeUncertainty:Boolean, includeReasons:Boolean) : DataContainer = {
    try{
    val timeRes = logTime("vistrailsQueryMimir") {
      logger.debug("vistrailsQueryMimir: " + query)
      val stmt = MimirSQL(query) match {
        case Parsed.Success(stmt, _) => stmt
        case f:Parsed.Failure => throw new Exception(s"Parse error: ${f.msg}")
      }
      stmt match {
        case SQLStatement(select:sparsity.statement.Select) => {
          val oper = db.sqlToRA(select)
          if(includeUncertainty){
            operCSVResultsDeterminism(oper)
          } else {
            operCSVResults(oper)
          }
        }
        case SQLStatement(update:sparsity.statement.Update) => {
          //db.backend.update(query)
          DataContainer(Seq(), Seq(Seq(StringPrimitive("SUCCESS")),Seq(IntPrimitive(1))), Seq(), Seq(), Seq(), Seq())
        }
        case _ => {
          db.update(stmt)
          DataContainer(Seq(), Seq(Seq(StringPrimitive("SUCCESS")),Seq(IntPrimitive(1))), Seq(), Seq(), Seq(), Seq())
        }
      }
      
    }
    logger.debug(s"vistrailsQueryMimir Took: ${timeRes._2}")
    timeRes._1
    } catch {
      case t: Throwable => {
        logger.error(s"Error Querying Mimir -> CSV: $query", t)
        throw t
      }
    }
  }
  
  def vistrailsQueryMimirJson(input:Any, query : String, includeUncertainty:Boolean, includeReasons:Boolean) : String = {
    val inputSubstitutionQuery = (input match {
        case aliases:JMapWrapper[_,_] => {
          registerNameMappings(aliases.asInstanceOf[JMapWrapper[String,String]])  
          query
        }
        case inputs:Seq[_] => {
          inputs.asInstanceOf[Seq[String]].zipWithIndex.foldLeft(query)((init, curr) => {
            init.replaceAll(s"\\{\\{\\s*input_${curr._2}\\s*\\}\\}", curr._1) 
          })
        }
        case _:String => {
          query.replaceAll("\\{\\{\\s*input[_0]*\\s*\\}\\}", input.toString)
        }
        case x => throw new Exception(s"Parameter type ${x.getClass()} is invalid for vistrailsQueryMimirJson input" )
    })
    vistrailsQueryMimirJson(inputSubstitutionQuery, includeUncertainty, includeReasons)
  }

  def vistrailsQueryMimirJson(query : String, includeUncertainty:Boolean, includeReasons:Boolean) : String = {
    try{
      val timeRes = logTime("vistrailsQueryMimirJson") {
        logger.debug("vistrailsQueryMimirJson: " + query)
        val stmt = MimirSQL(query) match {
          case Parsed.Success(stmt, _) => stmt
          case f:Parsed.Failure => throw new Exception(s"Parse Error: ${f.msg}")
        }
        stmt match {
          case SQLStatement(select:sparsity.statement.Select) => {
            val oper = db.sqlToRA(select)
            if(includeUncertainty)
              operCSVResultsDeterminismJson(oper)
            else 
              operCSVResultsJson(oper)
          }
          case SQLStatement(update:sparsity.statement.Update) => {
            //db.backend.update(query)
            JSONBuilder.dict(Map(
              "success" -> 0
            ))
          }
          case _ => {
            db.update(stmt)
            JSONBuilder.dict(Map(
              "success" -> 1
            ))
          }
        }
        
      }
      logger.debug(s"vistrailsQueryMimir Took: ${timeRes._2}")
      timeRes._1
    } catch {
      case t: Throwable => {
        logger.error(s"Error Querying Mimir -> JSON: $query", t)
        throw t
      }
    }
  }
  
  /*def vistrailsDeployWorkflowToViztool(input : Any, name:String, dataType:String, users:Seq[String], startTime:String, endTime:String, fields:String, latlonFields:String, houseNumberField:String, streetField:String, cityField:String, stateField:String, orderByFields:String) : String = {
    val timeRes = time {
      val inputTable = input.toString()
      val hash = ((inputTable + dataType + users.mkString("") + name + startTime + endTime).hashCode().toString().replace("-", "") )
      if(isWorkflowDeployed(hash)){
        logger.debug(s"vistrailsDeployWorkflowToViztool: workflow already deployed: $hash: $name")
      }
      else{
        val fieldsRegex = "\\s*(?:[a-zA-Z0-9_.]+\\s*(?:AS\\s+[a-zA-Z0-9_]+)?\\s*,\\s*)+[a-zA-Z0-9_.]+\\s*(?:AS\\s+[a-zA-Z0-9_]+)?\\s*".r
        val fieldRegex = "\\s*[a-zA-Z0-9_.]+\\s*(?:AS\\s+[a-zA-Z0-9_]+)?\\s*".r
        val fieldStr = fields.toUpperCase() match {
          case "" => "*"
          case "*" => "*"
          case fieldsRegex() => fields.toUpperCase()  
          case fieldRegex() => fields.toUpperCase()
          case x => throw new Exception("bad fields format: should be field, field")
        }
        val latLonFieldsRegex = "\\s*([a-zA-Z0-9_.]+)\\s*,\\s*([a-zA-Z0-9_.]+)\\s*".r
        val latlonFieldsSeq = latlonFields.toUpperCase() match {
          case "" | null => Seq("LATITUDE","LONGITUDE")
          case latLonFieldsRegex(latField, lonField) => Seq(latField, lonField)  
          case x => throw new Exception("bad fields format: should be latField, lonField")
        }
        val orderFieldsRegex = "\\s*(?:[a-zA-Z0-9_.]+\\s*,\\s*)?+[a-zA-Z0-9_.]+\\s*(?:DESC)?\\s*".r
        val orderBy = orderByFields.toUpperCase() match {
          case orderFieldsRegex() => "ORDER BY " + orderByFields.toUpperCase()  
          case x => ""
        }
        val query = s"SELECT $fieldStr FROM ${input} $orderBy"
        logger.debug("vistrailsDeployWorkflowToViztool: " + query + " users:" + users.mkString(","))
        if(startTime.matches("") && endTime.matches(""))
          deployWorkflowToViztool(hash, inputTable, query, name, dataType, users, latlonFieldsSeq, Seq(houseNumberField, streetField, cityField, stateField))
        else if(!startTime.matches(""))
          deployWorkflowToViztool(hash, inputTable, query, name, dataType, users, latlonFieldsSeq, Seq(houseNumberField, streetField, cityField, stateField), startTime)
        else if(!endTime.matches(""))
          deployWorkflowToViztool(hash, inputTable, query, name, dataType, users, latlonFieldsSeq, Seq(houseNumberField, streetField, cityField, stateField), endTime = endTime)
        else
          deployWorkflowToViztool(hash, inputTable, query, name, dataType, users, latlonFieldsSeq, Seq(houseNumberField, streetField, cityField, stateField), startTime, endTime)
      }
      input.toString()
    }
    logger.debug(s"vistrailsDeployWorkflowToViztool Took: ${timeRes._2}")
    timeRes._1
  }*/
  
    
  def explainSubsetWithoutSchema(query: String, rows:Seq[String], cols:Seq[String]) : Seq[mimir.ctables.ReasonSet] = {
    val oper = db.sqlToRA(MimirSQL.Select(query))
    explainSubsetWithoutSchema(oper, rows, cols)  
  } 
  def explainSubsetWithoutSchema(oper: Operator, rows:Seq[String], cols:Seq[String]) : Seq[mimir.ctables.ReasonSet] = {
    val timeRes = logTime("explainSubsetWithoutSchema") {
      logger.debug("explainSubsetWithoutSchema: From Vistrails: [ "+ rows +" ] [" + oper + "]"  ) ;
      val explCols = cols match {
        case Seq() => oper.columnNames
        case _ => cols.map { ID(_) }
      }
      rows.map(row => {
        db.uncertainty.explainSubsetWithoutOptimizing(
          db.uncertainty.filterByProvenance(db.compiler.optimize(oper),RowIdPrimitive(row)), 
          explCols.toSet, true, false, false)
      }).flatten
    }
    logger.debug(s"explainSubsetWithoutSchema Took: ${timeRes._2}")
    timeRes._1
  }  

  def explainSchema(query: String, cols:Seq[String]) : Seq[mimir.ctables.ReasonSet] = {
    val oper = db.sqlToRA(MimirSQL.Select(query))
    explainSchema(oper, cols)
  }  
  

  // Aug 2019 by OK: 
  //   Why do we need a specialized function for explaining adaptive schemas.  Shouldn't
  //   just regular view explanations be enough?
  def explainSchema(oper: Operator, cols:Seq[String]) : Seq[mimir.ctables.ReasonSet] = {
    val timeRes = logTime("explainSchema") {
      logger.debug("explainSchema: From Vistrails: [ "+ cols.mkString(",") +" ] [" + oper + "]"  ) ;
      val explCols = cols match {
        case Seq() => oper.columnNames
        case _ => cols.map { ID(_) }
      }
      db.uncertainty.explainSubset(
        oper = oper, 
        wantCol = explCols.toSet, 
        wantRow = false,
        wantSort = false,
        wantSchema = true
      )
    }
    logger.debug(s"explainSchema Took: ${timeRes._2}")
    timeRes._1
  }

  def explainCellJson(query: String, col:String, row:String) : String = {
    try{
      logger.debug("explainCell: From Vistrails: [" + col + "] [ "+ row +" ] [" + query + "]"  ) ;
      val oper = totallyOptimize(db.sqlToRA(MimirSQL.Select(query)))
      JSONBuilder.list(explainCell(oper, ID(col), RowIdPrimitive(row)).map(Json.toJson(_)))
    } catch {
      case t: Throwable => {
        logger.error("Error Explaining Cell: [" + col + "] [ "+ row +" ] [" + query + "]", t)
        throw t
      }
    }
  }
  
  def getSchema(query:String) : Seq[Schema] = {
    val timeRes = logTime("getSchema") {
      try{
        logger.debug("getSchema: From Vistrails: [" + query + "]"  ) ;
        val oper = totallyOptimize(db.sqlToRA(MimirSQL.Select(query)))
        db.typechecker.schemaOf(oper).map( schel =>  Schema( schel._1.toString(), schel._2.toString(), Type.rootType(schel._2).toString()))
      } catch {
        case t: Throwable => {
          logger.error("Error Getting Schema: [" + query + "]", t)
          throw t
        }
      } 
    }
    timeRes._1
  }
  
  def explainCell(query: String, col:String, row:String) : Seq[mimir.ctables.Reason] = explainCell(query, ID(col), row)
  
  def explainCell(query: String, col:ID, row:String) : Seq[mimir.ctables.Reason] = {
    try{
    logger.debug("explainCell: From Vistrails: [" + col + "] [ "+ row +" ] [" + query + "]"  ) ;
    val oper = totallyOptimize(db.sqlToRA(MimirSQL.Select(query)))
    explainCell(oper, col, RowIdPrimitive(row))
    } catch {
      case t: Throwable => {
        logger.error("Error Explaining Cell: [" + col + "] [ "+ row +" ] [" + query + "]", t)
        throw t
      }
    }
  }

  def explainCell(query: String, col:Int, row:String) : Seq[mimir.ctables.Reason] = {
    try{
    logger.debug("explainCell: From Vistrails: [" + col + "] [ "+ row +" ] [" + query + "]"  ) ;
    val oper = totallyOptimize(db.sqlToRA(MimirSQL.Select(query)))
    explainCell(oper, col, row)
    } catch {
      case t: Throwable => {
        logger.error("Error Explaining Cell: [" + col + "] [ "+ row +" ] [" + query + "]", t)
        throw t
      }
    }
  }
  
  def explainCell(oper: Operator, colIdx:Int, row:String) : Seq[mimir.ctables.Reason] = {
    try{
    val cols = oper.columnNames
    explainCell(oper, cols(colIdx), RowIdPrimitive(row))  
    } catch {
      case t: Throwable => {
        logger.error("Error Explaining Cell: [" + colIdx + "] [ "+ row +" ] [" + oper + "]", t)
        throw t
      }
    }
  }
  
  def explainCell(oper: Operator, col:ID, row:RowIdPrimitive) : Seq[mimir.ctables.Reason] = {
    try{
    val timeRes = logTime("explainCell") {
      try {
      logger.debug("explainCell: From Vistrails: [" + col + "] [ "+ row +" ] [" + oper + "]"  ) ;
      val provFilteredOper = db.uncertainty.filterByProvenance(oper,row)
      db.uncertainty.explainSubset(
          provFilteredOper, 
          Seq(col).toSet, false, false
        ).flatMap { _.summarize(db) }
      } catch {
          case t: Throwable => {
            t.printStackTrace() // TODO: handle error
            Seq[Reason]()
          }
        }
    }
    logger.debug(s"explainCell Took: ${timeRes._2}")
    val reasonStrs = timeRes._1.map { Json.toJson(_) } 
    val reasonStrsSet = reasonStrs.toSet
    //TODO: mike - remove this work-around for filtering out duplicate reasons for cell
    //  from shapedetector.  I think the problem is related to appliestocolumn because the
    //  reasons are duplicated the same number of times as there are number of columns
    //  that the shapedetector facet applies to.  I will fix this correctly after cidr.
    reasonStrsSet.map(reasonstr => 
      reasonStrs.indexOf(reasonstr)).toSeq.map(idx => timeRes._1(idx))
    } catch {
      case t: Throwable => {
        logger.error("Error Explaining Cell: [" + col + "] [ "+ row +" ] [" + oper + "]", t)
        throw t
      }
    }
  }
  
  def explainRow(query: String, row:String) : Seq[Reason] = {
    try{
    logger.debug("explainRow: From Vistrails: [ "+ row +" ] [" + query + "]"  ) ;
    val oper = totallyOptimize(db.sqlToRA(MimirSQL.Select(query)))
    explainRow(oper, row)  
    } catch {
      case t: Throwable => {
        logger.error("Error Explaining Row: [ "+ row +" ] [" + query + "]", t)
        throw t
      }
    }
  }
  
  def explainRow(oper: Operator, row:String) : Seq[Reason] = {
    try{
    val timeRes = logTime("explainRow") {
      logger.debug("explainRow: From Vistrails: [ "+ row +" ] [" + oper + "]"  ) ;
      val cols = oper.columnNames
      db.uncertainty.explainSubset(
          db.uncertainty.filterByProvenance(oper,RowIdPrimitive(row)), 
          Seq().toSet, true, false)
        .flatMap { _.summarize(db) }
    }
    logger.debug(s"explainRow Took: ${timeRes._2}")
    timeRes._1.distinct
    } catch {
      case t: Throwable => {
        logger.error("Error Explaining Row: [ "+ row +" ] [" + oper + "]", t)
        throw t
      }
    }
  }
  
  def explainSubset(query: String, rows:Seq[String], cols:Seq[ID]) : Seq[mimir.ctables.ReasonSet] = {
    try{
    logger.debug("explainSubset: From Vistrails: [ "+ rows +" ] [" + query + "]"  ) ;
    val oper = db.sqlToRA(MimirSQL.Select(query))
    explainSubset(oper, rows, cols)  
    } catch {
      case t: Throwable => {
        logger.error("Error Explaining Subset: [" + cols + "] [ "+ rows +" ] [" + query + "]", t)
        throw t
      }
    }  
  }
  
  def explainSubset(oper: Operator, rows:Seq[String], cols:Seq[ID]) : Seq[mimir.ctables.ReasonSet] = {
    try{
    val timeRes = logTime("explainSubset") {
      logger.debug("explainSubset: From Vistrails: [ "+ rows +" ] [" + oper + "]"  ) ;
      val explCols = cols match {
        case Seq() => oper.columnNames
        case _ => cols
      }
      rows.map(row => {
        db.uncertainty.explainSubset(
          db.uncertainty.filterByProvenance(oper,RowIdPrimitive(row)), 
          explCols.toSet, true, false)
      }).flatten
    }
    logger.debug(s"explainSubset Took: ${timeRes._2}")
    timeRes._1
    } catch {
      case t: Throwable => {
        logger.error("Error Explaining Subset: [" + cols + "] [ "+ rows +" ] [" + oper + "]", t)
        throw t
  }
    }    
  }

  def explainEverythingAllJson(query: String) : String = {
    try{
      logger.debug("explainCell: From Vistrails: [" + query + "]"  ) ;
      val oper = totallyOptimize(db.sqlToRA(MimirSQL.Select(query)))
      JSONBuilder.list(explainEverything(oper).map(_.all(db).toList).flatten.map(Json.toJson(_)))
    } catch {
      case t: Throwable => {
        logger.error("Error Explaining Cell: [" + query + "]", t)
        throw t
      }
    }
  }
  
  
  def explainEverythingJson(query: String) : String = {
    try{
      logger.debug("explainCell: From Vistrails: [" + query + "]"  ) ;
      val oper = totallyOptimize(db.sqlToRA(MimirSQL.Select(query)))
      Json.toJson(
        explainEverything(oper)
            .flatMap { _.summarize(db) }
      ).toString
    } catch {
      case t: Throwable => {
        logger.error("Error Explaining Cell: [" + query + "]", t)
        throw t
      }
    }
  }
  
  def explainEverythingAll(query: String) : Seq[Reason] = {
    try{
      logger.debug("explainEverything: From Vistrails: [" + query + "]"  ) ;
      val oper = db.sqlToRA(MimirSQL.Select(query))
      explainEverything(oper)
          .par
          .flatMap { rset => 
            Timer.monitor(s"Explaining everything for $rset", logger.trace(_)) {
              rset.summarize(db)
            }
          }
          .seq
    } catch {
      case t: Throwable => {
        logger.error("Error Explaining Everything: [" + query + "]", t)
        throw t
      }
    }  
  }
  
  def explainEverything(query: String) : Seq[mimir.ctables.ReasonSet] = {
    try{
    logger.debug("explainEverything: From Vistrails: [" + query + "]"  ) ;
    val oper = db.sqlToRA(MimirSQL.Select(query))
    explainEverything(oper)   
    } catch {
      case t: Throwable => {
        logger.error("Error Explaining Everything: [" + query + "]", t)
        throw t
      }
    }  
  }
  
  def explainEverything(oper: Operator) : Seq[mimir.ctables.ReasonSet] = {
    try{
    val timeRes = logTime("explainEverything") {
      logger.debug("explainEverything: From Vistrails: [" + oper + "]"  ) ;
      val cols = oper.columnNames
      db.uncertainty.explainEverything( oper)
    }
    logger.debug(s"explainEverything Took: ${timeRes._2}")
    timeRes._1
    } catch {
      case t: Throwable => {
        logger.error("Error Explaining Everything: [" + oper + "]", t)
        throw t
      }
    }    
  }
  
  def repairReason(reason: Reason) : Unit = {
    try{
    val timeRes = logTime("repairReason") {
      logger.debug("repairReason: From Vistrails: [ " + reason + " ]" ) ;
      reason.acknowledge(db)
    }
    logger.debug(s"repairReason Took: ${timeRes._2}")
    timeRes._1
    } catch {
      case t: Throwable => {
        logger.error("Error Repairing: [" + reason + " ]", t)
        throw t
      }
    }  
  }
  
  def feedback(reason: mimir.ctables.Reason) : Unit = {
    try {
      val timeRes = logTime("feedback") {
        logger.debug("feedback: From Vistrails: [ " + reason + " ]") ;
        reason.acknowledge(db)
      }
      logger.debug(s"feedback Took: ${timeRes._2}")
    } catch {
      case t: Throwable => {
        logger.error("Error with Feedback: [" + reason + " ]", t)
        throw t
      }
    }    
  }
  
  def getAvailableLenses() : Seq[String] = 
    db.lenses.monoLensTypes.map { _.id }.toSet.toSeq
  
  def getAvailableAdaptiveSchemas() : Seq[String] = 
    db.lenses.multiLensTypes.map { _.id }.toSet.toSeq
  
  def getAvailableViztoolUsers() : String = {
    var userIDs = Seq[String]()
    try{
      val ret = db.query(
        db.catalog.tableOperator(Name("USERS"))
          .project("USER_ID", "FIRST_NAME", "LAST_NAME")
      ) { results => 
        while(results.hasNext) {
          val row = results.next()
          userIDs = userIDs:+s"${row(0)}- ${row(1).asString} ${row(2).asString}"
        }
        userIDs.mkString(",") 
      }
      logger.debug(s"getAvailableViztoolUsers: From Viztrails: $ret")
      ret
    }catch {
      case t: Throwable => userIDs.mkString(",")
    }
  }
  
  def getAvailableViztoolDeployTypes() : String = {
    var types = Seq[String]("GIS", "DATA","INTERACTIVE")
    try {
      val ret = db.query(
        db.catalog.tableOperator(Name("CLEANING_JOBS"))
                  .project("TYPE")
      ) { results => 
        while(results.hasNext) {
          val row = results.next()
         types = types:+s"${row(0).asString}"
        }
        types.distinct.mkString(",")
      }
      logger.debug(s"getAvailableViztoolDeployTypes: From Viztrails: $ret")
      ret
    }catch {
      case t: Throwable => types.mkString(",")
    }
    
  }
  // End vistrails package defs
  //----------------------------------------------------------------------------------------------------
  
  def getTuple(oper: mimir.algebra.Operator) : Map[String,PrimitiveValue] = {
    db.query(oper)(results => {
      val cols = results.schema.map(f => f._1)
      val colsIndexes = results.schema.zipWithIndex.map( _._2)
      if(results.hasNext){
        val row = results.next()
        colsIndexes.map( (i) => {
           (cols(i).toString, row(i)) 
         }).toMap
      }
      else
        Map[String,PrimitiveValue]()
    })
  }
  
  def parseQuery(query:String) : Operator = {
    db.sqlToRA(MimirSQL.Select(query))
  }
  
  def operCSVResults(oper : mimir.algebra.Operator) : DataContainer =  {
    db.query(oper, UnannotatedBestGuess) { results => 
      val resCSV = scala.collection.mutable.Buffer[Seq[PrimitiveValue]]() 
      val prov = scala.collection.mutable.Buffer[String]()
      while(results.hasNext){
        val row = results.next()
        resCSV += row.tuple 
        prov += ""//row.provenance.asString
      }
      
      DataContainer(
        results.schema.map { f => Schema(f._1.toString, f._2.toString(), Type.rootType(f._2).toString()) },
        resCSV.toSeq, 
        prov.toSeq,
        Seq[Seq[Boolean]](), 
        Seq[Boolean](), 
        Seq() 
      )
    }
  }
  
 def operCSVResultsDeterminism(oper : mimir.algebra.Operator) : DataContainer =  {
   val ((schViz, cols, colsIndexes), (resCSV, colTaint, rowTaintProv)) = db.query(oper)( resIter => {
     val schstuf = resIter.schema.zipWithIndex.map(f => 
       (Schema(f._1._1.toString, f._1._2.toString(), Type.rootType(f._1._2).toString()), f._1._1.toString, f._2)).unzip3
     ((schstuf._1, schstuf._2, schstuf._3), resIter.toList.map( row => {
       (row.tuple, 
        schstuf._3.map(i => row.isColDeterministic(i)), 
        (row.isDeterministic(), row.provenance.asString))
     }).toSeq.unzip3)
   })
   val (rowTaint, prov) = rowTaintProv.unzip
   
   DataContainer(
      schViz,
      resCSV, 
      prov,
      colTaint, 
      rowTaint, 
      Seq() 
    )     
 }


  def operCSVResultsJson(oper : mimir.algebra.Operator) : String =  {
    db.query(oper, UnannotatedBestGuess)(results => {
      val resultList = results.toList
      val (resultsStrs, prov) = resultList.map(row => (row.tuple.map(cell => cell), row.provenance.asString)).unzip
      JSONBuilder.dict(Map(
        "schema" -> results.schema.map( schel =>  Map( "name" -> schel._1, "type" ->schel._2.toString(), "base_type" -> Type.rootType(schel._2).toString())),
        "data" -> resultsStrs,
        "prov" -> prov
      ))
    })
  }
  
  def operCSVResultsDeterminismJson(oper : mimir.algebra.Operator) : String =  {
    db.query(oper)(results => {
      val colsIndexes = results.schema.zipWithIndex.map( _._2)
      val resultList = results.toList 
      val (resultsStrsColTaint, provRowTaint) = resultList.map(row => ((row.tuple.map(cell => cell), colsIndexes.map(idx => row.isColDeterministic(idx).toString())), (row.provenance.asString, row.isDeterministic().toString()))).unzip
      val (resultsStrs, colTaint) = resultsStrsColTaint.unzip
      val (prov, rowTaint) = provRowTaint.unzip
      JSONBuilder.dict(Map(
        "schema" -> results.schema.map( schel =>  Map( "name" -> schel._1, "type" ->schel._2.toString(), "base_type" -> Type.rootType(schel._2).toString())),
        "data" -> resultsStrs,
        "prov" -> prov,
        "col_taint" -> colTaint,
        "row_taint" -> rowTaint
      ))
    }) 
  }
    
 def time[F](anonFunc: => F): (F, Long) = {  
      val tStart = System.nanoTime()
      val anonFuncRet = anonFunc  
      val tEnd = System.nanoTime()
      (anonFuncRet, tEnd-tStart)
    }  
 
 def logTime[F](name:String)(anonFunc: => F): (F, Long) = {
   val tStart = System.currentTimeMillis()
   val anonFuncRet = anonFunc  
   val tEnd = System.currentTimeMillis()
   if(ExperimentalOptions.isEnabled("LOGM")){
     val logFile =  new File(s"$VIZIER_DATA_PATH/logs/timing.log")
     if(!logFile.exists()){
       logFile.getParentFile().mkdirs()
       logFile.createNewFile()
     }
     val fw = new FileWriter(logFile, true) ; 
     fw.write(s"mimir, ${name}, ${UUID.randomUUID().toString}, duration, ${(tEnd-tStart)/1000.0}\n") ; 
     fw.close()
   }
   (anonFuncRet, tEnd-tStart)   
 }
 
 def totallyOptimize(oper : mimir.algebra.Operator) : mimir.algebra.Operator = {
    val preOpt = oper.toString() 
    val postOptOper = db.compiler.optimize(oper)
    val postOpt = postOptOper.toString() 
    if(preOpt.equals(postOpt))
      postOptOper
    else
      totallyOptimize(postOptOper)
  }
 
}


