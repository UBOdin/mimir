package mimir;

import java.io._
import java.util.Vector

import org.rogach.scallop._

import sparsity.Name
import sparsity.statement.CreateView
import sparsity.parser.{SQL,Expression} 

import fastparse.Parsed

import mimir.algebra._
import mimir.exec.result.Row
import mimir.backend.SparkBackend
import mimir.metadata.JDBCMetadataBackend
import mimir.util.ExperimentalOptions
//import net.sf.jsqlparser.statement.provenance.ProvenanceStatement
import mimir.exec.Compiler
import mimir.ctables.Reason
import org.slf4j.{LoggerFactory}
import ch.qos.logback.classic.{Level, Logger}
import com.typesafe.scalalogging.slf4j.LazyLogging
import mimir.serialization.Json
import mimir.util.LoggerUtils
import mimir.ml.spark.SparkML
import mimir.util.JSONBuilder
import java.util.UUID
import java.net.InetAddress
import scala.collection.convert.Wrappers.JMapWrapper
import mimir.algebra.spark.function.SparkFunctions
import java.net.URLDecoder
import mimir.parser._

import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox
import java.io.File
import mimir.exec.result.ResultIterator
import org.apache.spark.sql.SparkSession
import mimir.ctables.AnalyzeUncertainty
import mimir.parser.ExpressionParser
import mimir.ctables.MultiReason
import mimir.api.{ScalaEvalResponse, CreateLensResponse, DataContainer, Schema}
import mimir.api.MimirAPI

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
    Mimir.conf = new MimirConfig(args);
    ExperimentalOptions.enable(Mimir.conf.experimental())
    // Set up the database connection(s)
    val database = Mimir.conf.dbname().split("[\\\\/]").last.replaceAll("\\..*", "")
    val sback = new SparkBackend(database)
    db = new Database(sback, new JDBCMetadataBackend(Mimir.conf.backend(), Mimir.conf.dbname()))
    db.open()
    vizierdb.sparkSession = sback.sparkSql.sparkSession
    
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
       
      val mimirVizierLoggers = Seq("mimir.backend.SparkBackend", this.getClass.getName, 
          "mimir.api.MimirAPI", "mimir.api.MimirVizierServlet")
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
      db.backend.close()
      if(!Mimir.conf.quiet()) { logger.debug("\n\nDone.  Exiting."); }
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
  
  object vizierdb {
    
    var sparkSession:SparkSession = null;
    
    def withDataset[T](dsname:String, handler: ResultIterator => T ) : T = {
     val mimirName:ID = db.sqlToRA.getVizierNameMapping(ID(dsname)) match {
       case Some(mname) => mname
       case None => throw new Exception(s"No such table or view '$dsname'")
     }
     val oper = db.table(mimirName)
     db.query(oper)(handler)
    }
        
    def outputAnnotations(dsname:String) : String = {
      val mimirName:ID = db.sqlToRA.getVizierNameMapping(ID(dsname)) match {
       case Some(mname) => mname
       case None => throw new Exception(s"No such table or view '$dsname'")
     }
     val oper = db.table(mimirName)
     explainEverything(oper).mkString("<br><br>")
    }
  }
  
  //-------------------------------------------------
  //Mimir API impls
  ///////////////////////////////////////////////
  var apiCallThread : Thread = null
  def evalScala(source : String) : ScalaEvalResponse = {
    try {
      val timeRes = logTime("evalScala") {
        Eval("import mimir.MimirVizier.vizierdb\n"+source) : String
      }
      logger.debug(s"evalScala Took: ${timeRes._2}")
      ScalaEvalResponse(timeRes._1,"")
    } catch {
      case t: Throwable => {
        logger.error(s"Error Evaluating Scala Source", t)
        ScalaEvalResponse("",s"Error Evaluating Scala Source: \n${t.getMessage()}\n${t.getStackTrace.mkString("\n")}\n${t.getMessage()}\n${t.getCause.getStackTrace.mkString("\n")}")
      }
    }
  }
  
  def setSparkWorkers(count:Int):Unit = {
    db.backend.asInstanceOf[mimir.backend.SparkBackend].sparkSql.setConf("spark.sql.shuffle.partitions", count.toString())
  }
  
  def executeOnWorkers(oper:Operator, col:String):Unit = {
    val (compiledOp, cols, metadata) = mimir.exec.mode.BestGuess.rewrite(db, oper)
    val colidx = cols.indexOf(col)
    val dfRowFunc: (Iterator[org.apache.spark.sql.Row]) => Unit = (rows) => {
      rows.map(row => row.get(colidx))                                     
    }                                  
    db.backend.asInstanceOf[mimir.backend.SparkBackend].executeOnWorkers(compiledOp, dfRowFunc)                                                                                             
  }      
  
  def executeOnWorkersReturn(oper:Operator, col:String):Int = {
    import org.apache.spark.sql.functions.sum
    val (compiledOp, cols, metadata) = mimir.exec.mode.BestGuess.rewrite(db, oper)
    val colidx = cols.indexOf(col)
    val df = db.backend.asInstanceOf[mimir.backend.SparkBackend].execute(compiledOp)
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
    val detectHeaders = options.filter(_._1.equalsIgnoreCase("mimir_detect_headers")).foldLeft(true)( (init,cur) => init && (!cur._2.equalsIgnoreCase("false")))
    val inferTypes = options.filter(_._1.equalsIgnoreCase("mimir_infer_types")).foldLeft(true)( (init,cur) => init && (!cur._2.equalsIgnoreCase("false")))
    val backendOptions = options.filterNot(_._1.equalsIgnoreCase("mimir_detect_headers")).filterNot(_._1.equalsIgnoreCase("mimir_infer_types"))
    loadDataSource(file, format, inferTypes, detectHeaders, backendOptions)
  }
  def loadDataSource(file : String, format:String, inferTypes:Boolean, detectHeaders:Boolean, backendOptions:Seq[Any]) : String = {
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
      if(db.getAllTables().contains(tableName)){
        logger.debug("loadDataSource: From Vistrails: Table Already Exists: " + tableName)
      }
      else{
        db.loadTable(
          csvFile,
          targetTable = Some(tableName), 
          force = true, 
          targetSchema = None, 
          inferTypes = Some(inferTypes), 
          detectHeaders = Some(detectHeaders), 
          loadOptions = bkOpts.toMap, 
          format = ID(format)
        )
      }
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
  
  def unloadDataSource(input:String, file : String, format:String, backendOptions:Seq[Any]) : Unit = {
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
        val viewName =  db.sqlToRA.getVizierNameMapping(ID(input)) match {
          case Some(mimirName) => mimirName
          case None => ID(input)
        }
val df = db.backend.execute(db.compileBestGuess(db.table(viewName)))
        db.backend.writeDataSink(
            df, 
            format, 
            bkOpts.toMap, 
            if(file == null || file.isEmpty()) None else Some(file)
          )
      }
      logger.debug(s"unloadDataSource Took: ${timeRes._2}")
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
  
  def createLens(input : Any, params : java.util.ArrayList[String], _type : String, make_input_certain:Boolean, materialize:Boolean) : CreateLensResponse = {
    createLens(input, params.toArray[String](Array[String]()).toSeq, _type, make_input_certain, materialize)
  }
  
  def createLens(input : Any, params : Seq[String], _type : String, make_input_certain:Boolean, materialize:Boolean) : CreateLensResponse = {
    try{
    apiCallThread = Thread.currentThread()
    val timeRes = logTime("createLens") {
      logger.debug("createLens: From Vistrails: [" + input + "] [" + params.mkString(",") + "] [" + _type + "]"  ) ;
      
      val parsedParams =  // Start by replacing "{{input}}" with the name of the input table.
            params.map(param => 
              mimir.parser.ExpressionParser.expr( param.replaceAll("\\{\\{\\s*input\\s*\\}\\}", input.toString)) )
      val lensNameBase = (input.toString() + _type + parsedParams.mkString(",") + make_input_certain + materialize).hashCode()
      val inputQuery = s"SELECT * FROM ${input}"

      val lensName = "LENS_" + _type + (lensNameBase.toString().replace("-", ""))
      val lensType = _type.toUpperCase()

      if(db.tableExists(lensName)) {
        logger.debug("createLens: From Vistrails: Lens (or Table) already exists: " + lensName)
        // (Should be) safe to fall through since we might still be getting asked to materialize 
        // the table.
      } else {
        // Need to create the lens if it doesn't already exist.

        // query is a var because we might need to rewrite it below.
        var query:Operator = db.table(input.toString)
        // "Make Certain" is implemented by dumping the lens contents into a temporary table
        if(make_input_certain){
          val materializedInput = ID("MATERIALIZED_"+input)
          val querySchema = db.typechecker.schemaOf(query)

          if(db.tableExists(materializedInput)){
            logger.debug("createLens: From Vistrails: Materialized Input Already Exists: " + materializedInput)
          } else {
            // Dump the query into a table if necessary 
            db.backend.createTable(materializedInput, query)
          }

          // And override the default lens input with the table.
          query = db.table(materializedInput)
        }

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
          humanReadableName = Some(input.toString)
        )
      }
      if(materialize){
        if(!db.views(ID(lensName)).isMaterialized){
          db.views.materialize(ID(lensName))
        }
      }
      val lensOp = db.table(lensName).limit(200)
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
    db.sqlToRA.registerVizierNameMapping(Name(vizierName), ID(mimirName))
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
      db.getView(viewName) match {
        case None => {
          val viewQuery = SQL(inputSubstitutionQuery) match {
            case fastparse.Parsed.Success(sparsity.statement.Select(body, _), _) => body
            case x => throw new Exception(s"Invalid view query : $inputSubstitutionQuery \n $x")
          }
          logger.debug("createView: query: " + viewQuery)
          db.update(SQLStatement(CreateView(Name(viewName, true), false, viewQuery)))
        }
        case Some(_) => {
          logger.warn("createView: From Vistrails: View already exists: " + viewName)
        }
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
  
  def createAdaptiveSchema(input : Any, params : Seq[String], _type : String) : String = {
    try {
    apiCallThread = Thread.currentThread()
    val timeRes = logTime("createAdaptiveSchema") {
      logger.debug("createAdaptiveSchema: From Vistrails: [" + input + "] [" + params.mkString(",") + "]"  ) ;
      val paramExprs = params.map(param => 
        mimir.parser.ExpressionParser.expr( param.replaceAll("\\{\\{\\s*input\\s*\\}\\}", input.toString)) )
      val paramsStr = paramExprs.mkString(",")
      val adaptiveSchemaName = ID("ADAPTIVE_SCHEMA_" + _type + ((input.toString() + _type + paramsStr).hashCode().toString().replace("-", "") ))
      val asViewName = ID("VIEW_"+adaptiveSchemaName)
      db.getView(adaptiveSchemaName) match {
        case None => {
          db.adaptiveSchemas.create(adaptiveSchemaName, ID(_type), db.table(ID(input.toString)), paramExprs)
          val asTable = _type match {
            case "SHAPE_WATCHER" => adaptiveSchemaName
            case _ => ID("DATA")
          }
          db.views.create(asViewName, db.adaptiveSchemas.viewFor(adaptiveSchemaName, asTable).get)
        }
        case Some(_) => {
          logger.debug("createAdaptiveSchema: From Vistrails: Adaptive Schema already exists: " + adaptiveSchemaName)
        }
      }
      asViewName
    }
    logger.debug(s"createView ${timeRes._1.toString} Took: ${timeRes._2}")
    timeRes._1.toString
    } catch {
      case t: Throwable => {
        logger.error("Error Creating Adaptive Schema: [" + input + "] [" + params.mkString(",") + "]", t)
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
          (includeUncertainty, includeReasons) match {
            case (true, true) => operCSVResultsDeterminismAndExplanation(oper)
            case (true, false) => operCSVResultsDeterminism(oper)
            case _ => operCSVResults(oper)
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
    val inputSubstitutionQuery = input match {
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
      }
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
            if(includeUncertainty && includeReasons)
              operCSVResultsDeterminismAndExplanationJson(oper)
            else if(includeUncertainty)
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
  
  def explainSchema(oper: Operator, cols:Seq[String]) : Seq[mimir.ctables.ReasonSet] = {
    val timeRes = logTime("explainSchema") {
      logger.debug("explainSchema: From Vistrails: [ "+ cols.mkString(",") +" ] [" + oper + "]"  ) ;
      val explCols = cols match {
        case Seq() => oper.columnNames
        case _ => cols.map { ID(_) }
      }
      db.uncertainty.explainAdaptiveSchema(
          db.compiler.optimize(oper), 
          explCols.toSet, true)
    }
    logger.debug(s"explainSchema Took: ${timeRes._2}")
    timeRes._1
  }

  def explainCellJson(query: String, col:String, row:String) : String = {
    try{
      logger.debug("explainCell: From Vistrails: [" + col + "] [ "+ row +" ] [" + query + "]"  ) ;
      val oper = totallyOptimize(db.sqlToRA(MimirSQL.Select(query)))
      JSONBuilder.list(explainCell(oper, ID(col), RowIdPrimitive(row)).map(_.toJSON))
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
      val subsetReasons = db.uncertainty.explainSubset(
              provFilteredOper, 
              Seq(col).toSet, false, false)
      db.uncertainty.getFocusedReasons(subsetReasons)
      } catch {
          case t: Throwable => {
            t.printStackTrace() // TODO: handle error
            Seq[Reason]()
          }
        }
    }
    logger.debug(s"explainCell Took: ${timeRes._2}")
    timeRes._1
    } catch {
      case t: Throwable => {
        logger.error("Error Explaining Cell: [" + col + "] [ "+ row +" ] [" + oper + "]", t)
        throw t
      }
    }
  }
  
  def explainRow(query: String, row:String) : Seq[mimir.ctables.Reason] = {
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
  
  def explainRow(oper: Operator, row:String) : Seq[mimir.ctables.Reason] = {
    try{
    val timeRes = logTime("explainRow") {
      logger.debug("explainRow: From Vistrails: [ "+ row +" ] [" + oper + "]"  ) ;
      val cols = oper.columnNames
      db.uncertainty.getFocusedReasons(db.uncertainty.explainSubset(
              db.uncertainty.filterByProvenance(oper,RowIdPrimitive(row)), 
              Seq().toSet, true, false))
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
      JSONBuilder.list(explainEverything(oper).map(_.all(db).toList).flatten.map(_.toJSON))
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
      JSONBuilder.list(explainEverything(oper).map(rset => {
        val subReasons = rset.take(db, 4).toSeq
  			if(subReasons.size > 3){
  				logger.trace("   -> Too many explanations to fit in one group")
  				Seq(new MultiReason(db, rset))
  			} else {
  				logger.trace(s"   -> Only ${subReasons.size} explanations")
  				subReasons
  			}
        }).flatten.map(_.toJSON))
    } catch {
      case t: Throwable => {
        logger.error("Error Explaining Cell: [" + query + "]", t)
        throw t
      }
    }
  }
  
  def explainEverythingAll(query: String) : Seq[mimir.ctables.Reason] = {
    try{
    logger.debug("explainEverything: From Vistrails: [" + query + "]"  ) ;
    val oper = db.sqlToRA(MimirSQL.Select(query))
    explainEverything(oper).map(/*_.all(db).toList*/rset => {
        val subReasons = rset.take(db, 4).toSeq
  			if(subReasons.size > 3){
  				logger.trace("   -> Too many explanations to fit in one group")
  				Seq(new MultiReason(db, rset))
  			} else {
  				logger.trace(s"   -> Only ${subReasons.size} explanations")
  				subReasons
  			}}).flatten   
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
  
  def repairReason(reasons: Seq[mimir.ctables.Reason], idx:Int) : mimir.ctables.Repair = {
    try{
    val timeRes = logTime("repairReason") {
      logger.debug("repairReason: From Vistrails: [" + idx + "] [ " + reasons(idx) + " ]" ) ;
      reasons(idx).repair
    }
    logger.debug(s"repairReason Took: ${timeRes._2}")
    timeRes._1
    } catch {
      case t: Throwable => {
        logger.error("Error Repairing: [" + idx + "] [ " + reasons(idx) + " ]", t)
        throw t
      }
    }  
  }
  
  def feedback(reasons: Seq[mimir.ctables.Reason], idx:Int, ack: Boolean, repairStr: String) : Unit = {
    try{
    val timeRes = logTime("feedback") {
      logger.debug("feedback: From Vistrails: [" + idx + "] [ " + reasons(idx) + " ] [ " + ack + " ] [ " +repairStr+" ]" ) ;
      val reason = reasons(idx) 
      val argString = 
          if(!reason.args.isEmpty){
            " (" + reason.args.mkString(",") + ")"
          } else { "" }
      val update = if(ack) { reason.guess } else { db.sqlToRA(MimirSQL.Expression(repairStr)) }

      db.models.feedback(
        reason.model.name,
        reason.idx, 
        reason.args, 
        db.interpreter(update)
      )
    }
    logger.debug(s"feedback Took: ${timeRes._2}")
    } catch {
      case t: Throwable => {
        logger.error("Error with Feedback: [" + idx + "] [ " + reasons(idx) + " ] [ " + ack + " ] [ " +repairStr+" ]", t)
        throw t
      }
    }    
  }
  
  def feedback(model:String, idx:Int, argsHints:Seq[Any], ack: Boolean, repairStr: String) : Unit = {
    try{
    val timeRes = logTime("feedback") {
      logger.debug("feedback: From Vistrails: [" + idx + "] [ " + model + " ] [ " + argsHints.mkString(",") + " ] [ " + ack + " ] [ " +repairStr+" ]" ) ;
      val modelInst = db.models.get(ID(model))
      val splitIndex = modelInst.argTypes(idx).length
      val (args, hints) = argsHints.map(arg => ExpressionParser.expr(arg.toString()).asInstanceOf[PrimitiveValue]).splitAt(splitIndex)
      val update = if(ack) { modelInst.bestGuess(idx, args, hints) } else { db.sqlToRA(MimirSQL.Expression(repairStr)) }

      db.models.feedback(
        modelInst,
        idx, 
        args, 
        db.interpreter(update)
      )
    }
    logger.debug(s"feedback Took: ${timeRes._2}")
    } catch {
      case t: Throwable => {
        logger.error("Error with Feedback: [" + idx + "] [ " + model + " ] [ " + argsHints.mkString(",") + " ]  [ " + ack + " ] [ " +repairStr+" ]", t)
        throw t
      }
    }    
  }
  
  def getAvailableLenses() : Seq[String] = {
    val distinctLenseIdxs = db.lenses.lensTypes.toSeq.map(_._2).zipWithIndex.distinct.unzip._2
    val distinctLenses = db.lenses.lensTypes.toSeq.zipWithIndex.filter(el => distinctLenseIdxs.contains(el._2)).unzip._1.toMap
    val ret = distinctLenses.keySet.toSeq
    logger.debug(s"getAvailableLenses: From Viztrails: $ret")
    ret.map(_.toString())
  }
  
  def getAvailableAdaptiveSchemas() : Seq[String] = {
    val ret = mimir.adaptive.MultilensRegistry.multilenses.keySet.toSeq
    logger.debug(s"getAvailableAdaptiveSchemas: From Viztrails: $ret")
    ret.map(_.toString())
  }
  
  def getAvailableViztoolUsers() : String = {
    var userIDs = Seq[String]()
    try{
      val ret = db.query(
        db.table("USERS")
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
        db.table("CLEANING_JOBS")
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
    db.query(oper) { results => 
      val resCSV = scala.collection.mutable.Buffer[Seq[PrimitiveValue]]() 
      val prov = scala.collection.mutable.Buffer[String]()
      while(results.hasNext){
        val row = results.next()
        resCSV += row.tuple 
        prov += row.provenance.asString
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
 
 def operCSVResultsDeterminismAndExplanation(oper : mimir.algebra.Operator) : DataContainer =  {
   val ((schViz, cols, colsIndexes), (resCSV, colTaintReasons, rowTaintProv)) = db.query(oper)( resIter => {
     val schstuf = resIter.schema.zipWithIndex.map(f => 
       (Schema(f._1._1.toString, f._1._2.toString(), Type.rootType(f._1._2).toString()), f._1._1.toString(), f._2)).unzip3
     ((schstuf._1, schstuf._2, schstuf._3), resIter.toList.map( row => {
       (row.tuple, 
        schstuf._3.map(i => { if(row.isColDeterministic(i)){ (true, Seq()) } else { (false, explainCell(oper, ID(schstuf._2(i)), row.provenance)) } }).unzip, 
        (row.isDeterministic(), row.provenance.asString))
     }).toSeq.unzip3)
   })
   val (colTaint, reasons) = colTaintReasons.unzip
   val (rowTaint, prov) = rowTaintProv.unzip
   
   DataContainer(
      schViz,
      resCSV, 
      prov,
      colTaint, 
      rowTaint, 
      reasons.map(_.flatten.map(rsn => mimir.api.Reason(rsn.reason, rsn.model.name.toString(), rsn.idx, rsn.args.map(_.toString()), mimir.api.Repair(rsn.repair.toJSON), rsn.repair.exampleString) )) 
    )        
 }


  def operCSVResultsJson(oper : mimir.algebra.Operator) : String =  {
    db.query(oper)(results => {
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
 
 def operCSVResultsDeterminismAndExplanationJson(oper : mimir.algebra.Operator) : String =  {
     db.query(oper)(results => {
      val colsIndexes = results.schema.zipWithIndex.map( _._2)
      val resultList = results.toList 
      val (resultsStrsColTaint, provRowTaint) = resultList.map(row => ((row.tuple.map(cell => cell), colsIndexes.map(idx => row.isColDeterministic(idx).toString())), (row.provenance.asString, row.isDeterministic().toString()))).unzip
      val (resultsStrs, colTaint) = resultsStrsColTaint.unzip
      val (prov, rowTaint) = provRowTaint.unzip
      val reasons = explainEverything(oper).map(reasonSet => reasonSet.all(db).toSeq.map(_.toJSONWithFeedback))
      JSONBuilder.dict(Map(
        "schema" -> results.schema.map( schel =>  Map( "name" -> schel._1, "type" ->schel._2.toString(), "base_type" -> Type.rootType(schel._2).toString())),
        "data" -> resultsStrs,
        "prov" -> prov,
        "col_taint" -> colTaint,
        "row_taint" -> rowTaint,
        "reasons" -> reasons
      ))
    }) 
    
 } 

 /*def isWorkflowDeployed(hash:String) : Boolean = {
   db.query(Project(Seq(ProjectArg("CLEANING_JOB_ID",Var("CLEANING_JOB_ID"))) , mimir.algebra.Select( Comparison(Cmp.Eq, Var("HASH"), StringPrimitive(hash)), db.table("CLEANING_JOBS"))))( resIter => resIter.hasNext())
 }
                                                                                              //by default we'll start now and end when the galaxy class Enterprise launches
 def deployWorkflowToViztool(hash:String, input:String, query : String, name:String, dataType:String, users:Seq[String], latlonFields:Seq[String] = Seq("LATITUDE","LONGITUDE"), addrFields: Seq[String] = Seq("STRNUMBER", "STRNAME", "CITY", "STATE"), startTime:String = "2017-08-13 00:00:00", endTime:String = "2363-01-01 00:00:00") : Unit = {
   val backend = db.backend.asInstanceOf[InsertReturnKeyBackend]
   val jobID = backend.insertAndReturnKey(
       "INSERT INTO CLEANING_JOBS ( CLEANING_JOB_NAME, TYPE, IMAGE, HASH) VALUES ( ?, ?, ?, ? )", 
       Seq(StringPrimitive(name),StringPrimitive(dataType),StringPrimitive(s"app/images/$dataType.png"),StringPrimitive(hash))  
     )
   val dataID = backend.insertAndReturnKey(
       "INSERT INTO CLEANING_JOB_DATA ( CLEANING_JOB_ID, NAME, [QUERY] ) VALUES ( ?, ?, ? )",
       Seq(IntPrimitive(jobID),StringPrimitive(name),StringPrimitive(Json.ofOperator(parseQuery(query)).toString()))  
     )
   val datetimeprim = mimir.util.TextUtils.parseTimestamp(_)
   users.map(userID => {
     val schedID = backend.insertAndReturnKey(
         "INSERT INTO SCHEDULE_CLEANING_JOBS ( CLEANING_JOB_ID, START_TIME, END_TIME ) VALUES ( ?, ?, ? )",
         Seq(IntPrimitive(jobID),datetimeprim(startTime),datetimeprim(endTime))  
     )
     backend.insertAndReturnKey(
       "INSERT INTO SCHEDULE_USERS ( USER_ID, SCHEDULE_CLEANING_JOBS_ID, START_TIME, END_TIME ) VALUES ( ?, ?, ?, ? )",
       Seq(IntPrimitive(userID.split("-")(0).toLong),IntPrimitive(schedID),datetimeprim(startTime),datetimeprim(endTime))  
     )
   })
   dataType match {
     case "GIS" => {
       backend.insertAndReturnKey(
         "INSERT INTO CLEANING_JOB_SETTINGS_OPTIONS ( CLEANING_JOB_DATA_ID, TYPE, NAME, ID, OPTION ) VALUES ( ?, ?, ?, ?, ?)",
         Seq(IntPrimitive(dataID),StringPrimitive("GIS_LAT_LON_COLS"),StringPrimitive("Lat and Lon Columns"),StringPrimitive("LATLON"),StringPrimitive(s"""{"latCol":"${latlonFields(0)}", "lonCol":"${latlonFields(1)}" }"""))  
       )
       backend.insertAndReturnKey(
         "INSERT INTO CLEANING_JOB_SETTINGS_OPTIONS ( CLEANING_JOB_DATA_ID, TYPE, NAME, ID, OPTION ) VALUES ( ?, ?, ?, ?, ?)",
         Seq(IntPrimitive(dataID),StringPrimitive("GIS_ADDR_COLS"),StringPrimitive("Address Columns"),StringPrimitive("ADDR"),StringPrimitive(s"""{"houseNumber":"${addrFields(0)}", "street":"${addrFields(1)}", "city":"${addrFields(2)}", "state":"${addrFields(3)}" }"""))  
       )
       backend.insertAndReturnKey(
         "INSERT INTO CLEANING_JOB_SETTINGS_OPTIONS ( CLEANING_JOB_DATA_ID, TYPE, NAME, ID, OPTION ) VALUES ( ?, ?, ?, ?, ?)",
         Seq(IntPrimitive(dataID),StringPrimitive("LOCATION_FILTER"),StringPrimitive("Near Me"),StringPrimitive("NEAR_ME"),StringPrimitive(s"""{"distance":804.67,"latCol":"$input.${latlonFields(0)}","lonCol":"$input.${latlonFields(1)}"}"""))  
       )
       backend.insertAndReturnKey(
         "INSERT INTO CLEANING_JOB_SETTINGS_OPTIONS ( CLEANING_JOB_DATA_ID, TYPE, NAME, ID, OPTION ) VALUES ( ?, ?, ?, ?, ?)",
         Seq(IntPrimitive(dataID),StringPrimitive("MAP_CLUSTERER"),StringPrimitive("Cluster Markers"),StringPrimitive("CLUSTER"),StringPrimitive("{}"))  
       )
     }
     case "DATA" => {}
     case x => {}
   }
 }*/
 
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


