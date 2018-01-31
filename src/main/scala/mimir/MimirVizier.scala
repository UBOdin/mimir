package mimir;

import java.io._
import java.util.Vector

import org.rogach.scallop._

import mimir.algebra._
import mimir.exec.result.Row
import mimir.sql._
import mimir.util.ExperimentalOptions
//import net.sf.jsqlparser.statement.provenance.ProvenanceStatement
import net.sf.jsqlparser.statement.select.Select
import net.sf.jsqlparser.statement.update.Update
import py4j.GatewayServer
import mimir.exec.Compiler
import mimir.algebra.gprom.OperatorTranslation
import org.gprom.jdbc.jna.GProMWrapper
import mimir.ctables.Reason
import org.slf4j.{LoggerFactory}
import ch.qos.logback.classic.{Level, Logger}
import com.typesafe.scalalogging.slf4j.LazyLogging
import net.sf.jsqlparser.statement.Statement
import mimir.serialization.Json
import mimir.util.LoggerUtils

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

  var conf: MimirConfig = null;
  var db: Database = null;
  var gp: GProMBackend = null
  var usePrompt = true;
  var pythonMimirCallListeners = Seq[PythonMimirCallInterface]()

  def main(args: Array[String]) {
    conf = new MimirConfig(args);

    // Prepare experiments
    ExperimentalOptions.enable(conf.experimental())
    if(!ExperimentalOptions.isEnabled("GPROM-BACKEND")){
      // Set up the database connection(s)
      db = new Database(new JDBCBackend(conf.backend(), conf.dbname()))
      db.backend.open()
    }
    else {
      //Use GProM Backend
      gp = new GProMBackend(conf.backend(), conf.dbname(), 1)
      db = new Database(gp)    
      db.backend.open()
      gp.metadataLookupPlugin.db = db;
    }
    db.initializeDBForMimir();
    
    if(!ExperimentalOptions.isEnabled("NO-INLINE-VG")){
        db.backend.asInstanceOf[InlinableBackend].enableInlining(db)
      }
    
    OperatorTranslation.db = db       
    
   if(ExperimentalOptions.isEnabled("QUIET-LOG")){
      LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME) match {
          case logger: Logger => {
            logger.setLevel(Level.ERROR)
            logger.debug("root logger set to level: ERROR"); 
          }
        }
    }
    
    if(ExperimentalOptions.isEnabled("LOG")){
      LoggerFactory.getLogger(this.getClass.getName) match {
          case logger: Logger => {
            logger.setLevel(Level.DEBUG)
            logger.debug(this.getClass.getName +" logger set to level: DEBUG"); 
          }
        }
    }
    
    if(ExperimentalOptions.isEnabled("LOG-TRANSLATOR")){
      LoggerFactory.getLogger(OperatorTranslation.getClass.getName) match {
          case logger: Logger => {
            logger.setLevel(Level.DEBUG)
            logger.debug(OperatorTranslation.getClass.getName +" logger set to level: DEBUG"); 
          }
        }
    }
    
    //val gpromNode = GProMWrapper.inst.rewriteQueryToOperatorModel("SELECT SUM(COLUMN_0) AS FCOL_2, SUM(COLUMN_2) FROM R_RAW GROUP BY COLUMN_1;")
    //val gpNodeStr = GProMWrapper.inst.gpromNodeToString(gpromNode.getPointer())
    //println(gpNodeStr)
    //vistrailsQueryMimir("SELECT * FROM LENS_REPAIR_KEY1915024710", true, false)
    //vistrailsQueryMimir("SELECT * FROM LENS_COMMENT2063309830", true, false)
    //explainCell("SELECT * FROM LENS_REPAIR_KEY1915024710", 1, "2" )
    //explainEverything("SELECT * FROM LENS_REPAIR_KEY1915024710")
    //OperatorTranslation.compileProvenanceWithGProM(db.table("R_RAW").project("COLUMN_0"))
    /*val oper = db.sql.convert(db.parse("SELECT * FROM LENS_REPAIR_KEY1915024710").head.asInstanceOf[Select])
    val (provOp, provCols) = OperatorTranslation.compileProvenanceWithGProM(oper)
    val (provTaintOp, colTaintExprs, rowTaintExpr) = OperatorTranslation.compileTaintWithGProM(provOp)*/
    //OperatorTranslation.compileProvenanceAndTaintWithGProM(oper)
    //explainCell("SELECT * FROM LENS_COMMENT2063309830", 3, "5")
    /*val oper = db.sql.convert(db.parse("SELECT * FROM LENS_REPAIR_KEY1915024710").head.asInstanceOf[Select])
    OperatorTranslation.compileProvenanceAndTaintWithGProM(oper)*/
    //loadCSV("test/r_test/r.csv")
    
    /*val gpromNode = GProMWrapper.inst.rewriteQueryToOperatorModel("PROVENANCE OF (SELECT SUM(COLUMN_0) AS FCOL_2, SUM(COLUMN_2) FROM R_RAW GROUP BY COLUMN_1);")
    val provGpromNode = GProMWrapper.inst.provRewriteOperator(gpromNode.getPointer)
    //val provNodeStr = GProMWrapper.inst.gpromNodeToString(provGpromNode.getPointer())
    //println(provNodeStr)
    var opOut = OperatorTranslation.gpromStructureToMimirOperator(0, provGpromNode, null)
    println(opOut)*/
    
    
    //mimir lens examples
    /*vistrailsQueryMimir("SELECT * FROM LENS_MISSING_VALUE1225222496", true, false)  
    vistrailsQueryMimir("SELECT * FROM LENS_REPAIR_KEY1915024710", true, false) 
    vistrailsQueryMimir("SELECT * FROM LENS_COMMENT2063309830", true, false) 
    vistrailsQueryMimir("SELECT * FROM LENS_MISSING_KEY731940496", true, false) 
    vistrailsQueryMimir("SELECT * FROM LENS_PICKER502036449", true, false) */
    //vistrailsQueryMimir("SELECT * FROM LENS_GEOCODE97197618", true, false) 
    
    /*explainCell("SELECT * FROM LENS_MISSING_VALUE1225222496", 1, "3" )
    explainCell("SELECT * FROM LENS_REPAIR_KEY1915024710", 1, "2" ) 
    explainCell("SELECT * FROM LENS_COMMENT2063309830", 3, "2" ) 
    explainCell("SELECT * FROM LENS_PICKER502036449", 3, "3" ) */
    //explainCell("SELECT * FROM LENS_GEOCODE97197618", 4, "4" ) 
    //explainCell("SELECT * FROM LENS_REPAIR_KEY1915024710", 1, "1" ) 
     
    
    
    if(!ExperimentalOptions.isEnabled("NO-VISTRAILS")){
      runServerForViztrails()
      db.backend.close()
      if(!conf.quiet()) { logger.debug("\n\nDone.  Exiting."); }
    }
    
    
  }
  
  private var mainThread : Thread = null
  private var pythonGatewayRunning : Boolean = true 
  def shutdown() : Unit = {
    this.synchronized{
      pythonGatewayRunning = false
      pythonCallThread = null
      mainThread.interrupt()
      mainThread = null
    }
  }
  
  def isPythonGatewayRunning() : Boolean = {
    this.synchronized{
      pythonGatewayRunning
    }
  }
  
  def runServerForViztrails() : Unit = {
     mainThread = Thread.currentThread()
     val server = new GatewayServer(this, 33388)
     server.addListener(new py4j.GatewayServerListener(){
        def connectionError(connExept : java.lang.Exception) = {
          logger.debug("Python GatewayServer connectionError: " + connExept)
        }
  
        def connectionStarted(conn : py4j.Py4JServerConnection) = {
          logger.debug("Python GatewayServer connectionStarted: " + conn)
        }
        
        def connectionStopped(conn : py4j.Py4JServerConnection) = {
          logger.debug("Python GatewayServer connectionStopped: " + conn)
        }
        
        def serverError(except: java.lang.Exception) = {
          logger.debug("Python GatewayServer serverError")
        }
        
        def serverPostShutdown() = {
           logger.debug("Python GatewayServer serverPostShutdown")
        }
        
        def serverPreShutdown() = {
           logger.debug("Python GatewayServer serverPreShutdown")
        }
        
        def serverStarted() = {
           logger.debug("Python GatewayServer serverStarted")
        }
        
        def serverStopped() = {
           logger.debug("Python GatewayServer serverStopped")
        }
     })
     server.start()
     
     while(isPythonGatewayRunning()){
       Thread.sleep(90000)
       if(pythonCallThread != null){
         //logger.debug("Python Call Thread Stack Trace: ---------v ")
         //pythonCallThread.getStackTrace.foreach(ste => logger.debug(ste.toString()))
       }
       pythonMimirCallListeners.foreach(listener => {
       
          //logger.debug(listener.callToPython("knock knock, jvm here"))
         })
     }
     Thread.sleep(1000)
     server.shutdown()
    
  }
  
  //-------------------------------------------------
  //Python package defs
  ///////////////////////////////////////////////
  var pythonCallThread : Thread = null
  def loadCSV(file : String) : String = loadCSV(file, ("CSV", Seq(StringPrimitive(","))))
  def loadCSV(file : String, delimeter:String, inferTypes:Boolean, detectHeaders:Boolean) : String = 
    loadCSV(file, ("CSV", Seq(StringPrimitive(delimeter), BoolPrimitive(inferTypes), BoolPrimitive(detectHeaders))))
  def loadCSV(file : String, format:(String, Seq[PrimitiveValue])) : String = {
    pythonCallThread = Thread.currentThread()
    val timeRes = time {
      logger.debug("loadCSV: From Vistrails: [" + file + "]") ;
      val csvFile = new File(file)
      val nameFromFile = csvFile.getName().split("\\.")(0)
      val tableName = (csvFile.getName().split("\\.")(0) ).toUpperCase
      if(db.getAllTables().contains(tableName)){
        logger.debug("loadCSV: From Vistrails: Table Already Exists: " + tableName)
      }
      else{
        db.loadTable( nameFromFile, csvFile,  true, format)
      }
      tableName 
    }
    logger.debug(s"loadCSV Took: ${timeRes._2}")
    timeRes._1
  }
  
  def createLens(input : Any, params : Seq[String], _type : String, make_input_certain:Boolean, materialize:Boolean) : String = {
    pythonCallThread = Thread.currentThread()
    val timeRes = time {
      logger.debug("createLens: From Vistrails: [" + input + "] [" + params.mkString(",") + "] [" + _type + "]"  ) ;
      val paramsStr = params.mkString(",").replaceAll("\\{\\{\\s*input\\s*\\}\\}", input.toString) 
      val lenseName = "LENS_" + _type + ((input.toString() + _type + paramsStr + make_input_certain + materialize).hashCode().toString().replace("-", "") )
      var query:String = null
      db.getView(lenseName) match {
        case None => {
          if(make_input_certain){
            val materializedInput = "MATERIALIZED_"+input
            query = s"CREATE LENS ${lenseName} AS SELECT * FROM ${materializedInput} WITH ${_type}(${paramsStr})"  
            if(db.getAllTables().contains(materializedInput)){
                logger.debug("createLens: From Vistrails: Materialized Input Already Exists: " + materializedInput)
            }
            else{  
              val inputQuery = s"SELECT * FROM ${input}"
              val oper = db.sql.convert(db.parse(inputQuery).head.asInstanceOf[Select])
              db.selectInto(materializedInput, oper)
            }
          }
          else{
            val inputQuery = s"SELECT * FROM ${input}"
            query = s"CREATE LENS ${lenseName} AS $inputQuery WITH ${_type}(${paramsStr})"  
          }
          logger.debug("createLens: query: " + query)
          db.update(db.parse(query).head) 
        }
        case Some(_) => {
          logger.debug("createLens: From Vistrails: Lens already exists: " + lenseName)
        }
      }
      if(materialize){
        if(!db.views(lenseName).isMaterialized)
          db.update(db.parse(s"ALTER VIEW ${lenseName} MATERIALIZE").head)
      }
      lenseName
    }
    logger.debug(s"createLens ${_type} Took: ${timeRes._2}")
    timeRes._1
  }
  
  def createView(input : Any, query : String) : String = {
    pythonCallThread = Thread.currentThread()
    val timeRes = time {
      logger.debug("createView: From Vistrails: [" + input + "] [" + query + "]"  ) ;
      val inputSubstitutionQuery = query.replaceAll("\\{\\{\\s*input\\s*\\}\\}", input.toString) 
      val viewName = "VIEW_" + ((input.toString() + query).hashCode().toString().replace("-", "") )
      db.getView(viewName) match {
        case None => {
          val viewQuery = s"CREATE VIEW $viewName AS $inputSubstitutionQuery"  
          logger.debug("createView: query: " + viewQuery)
          db.update(db.parse(viewQuery).head)
        }
        case Some(_) => {
          logger.debug("createView: From Vistrails: View already exists: " + viewName)
        }
      }
      viewName
    }
    logger.debug(s"createView Took: ${timeRes._2}")
    timeRes._1
  }
  
  def createAdaptiveSchema(input : Any, params : Seq[String], _type : String) : String = {
    pythonCallThread = Thread.currentThread()
    val timeRes = time {
      logger.debug("createAdaptiveSchema: From Vistrails: [" + input + "] [" + params.mkString(",") + "]"  ) ;
      val paramExprs = params.map(param => 
        mimir.parser.ExpressionParser.expr( param.replaceAll("\\{\\{\\s*input\\s*\\}\\}", input.toString)) )
      val paramsStr = paramExprs.mkString(",")
      val adaptiveSchemaName = "ADAPTIVE_SCHEMA_" + _type + ((input.toString() + _type + paramsStr).hashCode().toString().replace("-", "") )
      db.getView(adaptiveSchemaName) match {
        case None => {
          db.adaptiveSchemas.create(adaptiveSchemaName, _type, db.table(input.toString), paramExprs)
          db.views.create("VIEW_"+adaptiveSchemaName, db.adaptiveSchemas.viewFor(adaptiveSchemaName, "DATA").get)
        }
        case Some(_) => {
          logger.debug("createAdaptiveSchema: From Vistrails: Adaptive Schema already exists: " + adaptiveSchemaName)
        }
      }
      "VIEW_"+adaptiveSchemaName
    }
    logger.debug(s"createView Took: ${timeRes._2}")
    timeRes._1
  }
  
  def vistrailsQueryMimir(query : String, includeUncertainty:Boolean, includeReasons:Boolean) : PythonCSVContainer = {
    val timeRes = time {
      logger.debug("vistrailsQueryMimir: " + query)
      val jsqlStmnt = db.parse(query).head
      jsqlStmnt match {
        case select:Select => {
          val oper = db.sql.convert(select)
          if(includeUncertainty && includeReasons)
            operCSVResultsDeterminismAndExplanation(oper)
          else if(includeUncertainty)
            operCSVResultsDeterminism(oper)
          else 
            operCSVResults(oper)
        }
        case update:Update => {
          db.backend.update(query)
          new PythonCSVContainer("SUCCESS\n1", Array(Array()), Array(), Array(), Array(), Map())
        }
        case stmt:Statement => {
          db.update(stmt)
          new PythonCSVContainer("SUCCESS\n1", Array(Array()), Array(), Array(), Array(), Map())
        }
      }
      
    }
    logger.debug(s"vistrailsQueryMimir Took: ${timeRes._2}")
    timeRes._1
  }
  
  def vistrailsDeployWorkflowToViztool(input : Any, name:String, dataType:String, users:Seq[String], startTime:String, endTime:String, fields:String, latlonFields:String, houseNumberField:String, streetField:String, cityField:String, stateField:String, orderByFields:String) : String = {
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
        val orderFieldsRegex = "\\s*(?:[a-zA-Z0-9_.]+\\s*,\\s*)+[a-zA-Z0-9_.]+\\s*(?:DESC)?\\s*".r
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
  }
  
  def explainCell(query: String, col:Int, row:String) : Seq[mimir.ctables.Reason] = {
    logger.debug("explainCell: From Vistrails: [" + col + "] [ "+ row +" ] [" + query + "]"  ) ;
    val oper = totallyOptimize(db.sql.convert(db.parse(query).head.asInstanceOf[Select]))
    explainCell(oper, col, row)
  }
  
  def explainCell(oper: Operator, colIdx:Int, row:String) : Seq[mimir.ctables.Reason] = {
    val cols = oper.columnNames
    explainCell(oper, cols(colIdx), RowIdPrimitive(row))  
  }
  
  def explainCell(oper: Operator, col:String, row:RowIdPrimitive) : Seq[mimir.ctables.Reason] = {
    val timeRes = time {
      try {
      logger.debug("explainCell: From Vistrails: [" + col + "] [ "+ row +" ] [" + oper + "]"  ) ;
      val provFilteredOper = db.explainer.filterByProvenance(oper,row)
      val subsetReasons = db.explainer.explainSubset(
              provFilteredOper, 
              Seq(col).toSet, false, false)
      db.explainer.getFocusedReasons(subsetReasons)
      } catch {
          case t: Throwable => {
            t.printStackTrace() // TODO: handle error
            Seq[Reason]()
          }
        }
    }
    logger.debug(s"explainCell Took: ${timeRes._2}")
    timeRes._1
  }
  
  def explainRow(query: String, row:String) : Seq[mimir.ctables.Reason] = {
    logger.debug("explainRow: From Vistrails: [ "+ row +" ] [" + query + "]"  ) ;
    val oper = totallyOptimize(db.sql.convert(db.parse(query).head.asInstanceOf[Select]))
    explainRow(oper, row)  
  }
  
  def explainRow(oper: Operator, row:String) : Seq[mimir.ctables.Reason] = {
    val timeRes = time {
      logger.debug("explainRow: From Vistrails: [ "+ row +" ] [" + oper + "]"  ) ;
      val cols = oper.columnNames
      db.explainer.getFocusedReasons(db.explainer.explainSubset(
              db.explainer.filterByProvenance(oper,RowIdPrimitive(row)), 
              Seq().toSet, true, false))
    }
    logger.debug(s"explainRow Took: ${timeRes._2}")
    timeRes._1.distinct
  }
  
  def explainSubset(query: String, rows:Seq[String], cols:Seq[String]) : Seq[mimir.ctables.ReasonSet] = {
    logger.debug("explainSubset: From Vistrails: [ "+ rows +" ] [" + query + "]"  ) ;
    val oper = db.sql.convert(db.parse(query).head.asInstanceOf[Select])
    explainSubset(oper, rows, cols)  
  }
  
  def explainSubset(oper: Operator, rows:Seq[String], cols:Seq[String]) : Seq[mimir.ctables.ReasonSet] = {
    val timeRes = time {
      logger.debug("explainSubset: From Vistrails: [ "+ rows +" ] [" + oper + "]"  ) ;
      val explCols = cols match {
        case Seq() => oper.columnNames
        case _ => cols
      }
      rows.map(row => {
        db.explainer.explainSubset(
          db.explainer.filterByProvenance(oper,RowIdPrimitive(row)), 
          explCols.toSet, true, false)
      }).flatten
    }
    logger.debug(s"explainSubset Took: ${timeRes._2}")
    timeRes._1
  }
  
  def explainSubsetWithoutSchema(query: String, rows:Seq[String], cols:Seq[String]) : Seq[mimir.ctables.ReasonSet] = {
    val oper = db.sql.convert(db.parse(query).head.asInstanceOf[Select])
    explainSubsetWithoutSchema(oper, rows, cols)  
  } 
  def explainSubsetWithoutSchema(oper: Operator, rows:Seq[String], cols:Seq[String]) : Seq[mimir.ctables.ReasonSet] = {
    val timeRes = time {
      logger.debug("explainSubsetWithoutSchema: From Vistrails: [ "+ rows +" ] [" + oper + "]"  ) ;
      val explCols = cols match {
        case Seq() => oper.columnNames
        case _ => cols
      }
      rows.map(row => {
        db.explainer.explainSubsetWithoutOptimizing(
          db.explainer.filterByProvenance(db.compiler.optimize(oper),RowIdPrimitive(row)), 
          explCols.toSet, true, false, false)
      }).flatten
    }
    logger.debug(s"explainSubsetWithoutSchema Took: ${timeRes._2}")
    timeRes._1
  }
  
  def explainSchema(query: String, cols:Seq[String]) : Seq[mimir.ctables.ReasonSet] = {
    val oper = db.sql.convert(db.parse(query).head.asInstanceOf[Select])
    explainSchema(oper, cols)  
  }  
  def explainSchema(oper: Operator, cols:Seq[String]) : Seq[mimir.ctables.ReasonSet] = {
    val timeRes = time {
      logger.debug("explainSchema: From Vistrails: [ "+ cols.mkString(",") +" ] [" + oper + "]"  ) ;
      val explCols = cols match {
        case Seq() => oper.columnNames
        case _ => cols
      }
      db.explainer.explainAdaptiveSchema(
          db.compiler.optimize(oper), 
          explCols.toSet, true)
    }
    logger.debug(s"explainSchema Took: ${timeRes._2}")
    timeRes._1
  }

  def explainEverything(query: String) : Seq[mimir.ctables.ReasonSet] = {
    logger.debug("explainEverything: From Vistrails: [" + query + "]"  ) ;
    val oper = db.sql.convert(db.parse(query).head.asInstanceOf[Select])
    explainEverything(oper)   
  }
  
  def explainEverything(oper: Operator) : Seq[mimir.ctables.ReasonSet] = {
    val timeRes = time {
      logger.debug("explainEverything: From Vistrails: [" + oper + "]"  ) ;
      val cols = oper.columnNames
      db.explainer.explainEverything( oper)
    }
    logger.debug(s"explainEverything Took: ${timeRes._2}")
    timeRes._1
  }
  
  def repairReason(reasons: Seq[mimir.ctables.Reason], idx:Int) : mimir.ctables.Repair = {
    val timeRes = time {
      logger.debug("repairReason: From Vistrails: [" + idx + "] [ " + reasons(idx) + " ]" ) ;
      reasons(idx).repair
    }
    logger.debug(s"repairReason Took: ${timeRes._2}")
    timeRes._1
  }
  
  def feedback(reasons: Seq[mimir.ctables.Reason], idx:Int, ack: Boolean, repairStr: String) : Unit = {
    val timeRes = time {
      logger.debug("feedback: From Vistrails: [" + idx + "] [ " + reasons(idx) + " ] [ " + ack + " ] [ " +repairStr+" ]" ) ;
      val reason = reasons(idx) 
      val argString = 
          if(!reason.args.isEmpty){
            " (" + reason.args.mkString(",") + ")"
          } else { "" }
      if(ack)
        db.update(db.parse(s"FEEDBACK ${reason.model.name} ${reason.idx}$argString IS ${ reason.guess }").head)
      else 
        db.update(db.parse(s"FEEDBACK ${reason.model.name} ${reason.idx}$argString IS ${ repairStr }").head)
    }
    logger.debug(s"feedback Took: ${timeRes._2}")
  }
  
  def registerPythonMimirCallListener(listener : PythonMimirCallInterface) = {
    logger.debug("registerPythonMimirCallListener: From Vistrails: ") ;
    pythonMimirCallListeners = pythonMimirCallListeners.union(Seq(listener))
  }
  
  def getAvailableLenses() : String = {
    val ret = db.lenses.lensTypes.keySet.toSeq.mkString(",")
    logger.debug(s"getAvailableLenses: From Viztrails: $ret")
    ret
  }
  
  def getAvailableAdaptiveSchemas() : String = {
    val ret = mimir.adaptive.MultilensRegistry.multilenses.keySet.toSeq.mkString(",")
    logger.debug(s"getAvailableAdaptiveSchemas: From Viztrails: $ret")
    ret
  }
  
  def getAvailableViztoolUsers() : String = {
    var userIDs = Seq[String]()
    try{
      val ret = db.query(Project(Seq(ProjectArg("USER_ID",Var("USER_ID")),ProjectArg("FIRST_NAME",Var("FIRST_NAME")),ProjectArg("LAST_NAME",Var("LAST_NAME"))), db.table("USERS")))(results => {
      while(results.hasNext) {
        val row = results.next()
        userIDs = userIDs:+s"${row(0)}- ${row(1).asString} ${row(2).asString}"
      }
      userIDs.mkString(",") 
      })
      logger.debug(s"getAvailableViztoolUsers: From Viztrails: $ret")
      ret
    }catch {
      case t: Throwable => userIDs.mkString(",")
    }
  }
  
  def getAvailableViztoolDeployTypes() : String = {
    var types = Seq[String]("GIS", "DATA","INTERACTIVE")
    try {
      val ret = db.query(Project(Seq(ProjectArg("TYPE",Var("TYPE"))), db.table("CLEANING_JOBS")))(results => {
      while(results.hasNext) {
        val row = results.next()
       types = types:+s"${row(0).asString}"
      }
      types.distinct.mkString(",")
      })
      logger.debug(s"getAvailableViztoolDeployTypes: From Viztrails: $ret")
      ret
    }catch {
      case t: Throwable => types.mkString(",")
    }
    
  }
  
  def getTuple(oper: mimir.algebra.Operator) : Map[String,PrimitiveValue] = {
    db.query(oper)(results => {
      val cols = results.schema.map(f => f._1)
      val colsIndexes = results.schema.zipWithIndex.map( _._2)
      if(results.hasNext){
        val row = results.next()
        colsIndexes.map( (i) => {
           (cols(i), row(i)) 
         }).toMap
      }
      else
        Map[String,PrimitiveValue]()
    })
  }
  
  def parseQuery(query:String) : Operator = {
    db.sql.convert(db.parse(query).head.asInstanceOf[Select])
  }
  
  def operCSVResults(oper : mimir.algebra.Operator) : PythonCSVContainer =  {
    db.query(oper)(results => {
    val cols = results.schema.map(f => f._1)
    val colsIndexes = results.schema.zipWithIndex.map( _._2)
    val rows = new StringBuffer()
    val prov = new Vector[String]()
    while(results.hasNext){
      val row = results.next()
      rows.append(colsIndexes.map( (i) => {
         row(i).toString 
       }).mkString(", ")).append("\n")
       prov.add(row.provenance.asString)
    }
    val resCSV = cols.mkString(", ") + "\n" + rows.toString()
    new PythonCSVContainer(resCSV, Array[Array[Boolean]](), Array[Boolean](), Array[Array[String]](), prov.toArray[String](Array()), results.schema.map(f => (f._1, f._2.toString())).toMap)
    })
  }
  
 def operCSVResultsDeterminism(oper : mimir.algebra.Operator) : PythonCSVContainer =  {
     val results = new Vector[Row]()
     var cols : Seq[String] = null
     var colsIndexes : Seq[Int] = null
     var schViz : Map[String, String] = null
     
     db.query(oper)( resIter => {
         schViz = resIter.schema.map(f => (f._1, f._2.toString())).toMap
         cols = resIter.schema.map(f => f._1)
         colsIndexes = resIter.schema.zipWithIndex.map( _._2)
         while(resIter.hasNext())
           results.add(resIter.next)
     })
     val resCSV = results.toArray[Row](Array()).seq.map(row => {
       val truples = colsIndexes.map( (i) => {
         (row(i).toString, row.isColDeterministic(i)) 
       }).unzip
       (truples._1.mkString(", "), truples._2.toArray, (row.isDeterministic(), row.provenance.asString))
     }).unzip3
     val rowDetAndProv = resCSV._3.unzip
     new PythonCSVContainer(resCSV._1.mkString(cols.mkString(", ") + "\n", "\n", ""), resCSV._2.toArray, rowDetAndProv._1.toArray, Array[Array[String]](), rowDetAndProv._2.toArray, schViz)
  }
 
 def operCSVResultsDeterminismAndExplanation(oper : mimir.algebra.Operator) : PythonCSVContainer =  {
     val results = new Vector[Row]()
     var cols : Seq[String] = null
     var colsIndexes : Seq[Int] = null
     var schViz : Map[String, String] = null
     
     db.query(oper)( resIter => {
         schViz = resIter.schema.map(f => (f._1, f._2.toString())).toMap
         cols = resIter.schema.map(f => f._1)
         colsIndexes = resIter.schema.zipWithIndex.map( _._2)
         while(resIter.hasNext())
           results.add(resIter.next)
     })
     val resCSV = results.toArray[Row](Array()).seq.map(row => {
       val truples = colsIndexes.map( (i) => {
         (row(i).toString, row.isColDeterministic(i), if(!row.isColDeterministic(i))explainCell(oper, cols(i), row.provenance).mkString(",")else"") 
       }).unzip3
       (truples._1.mkString(", "), (truples._2.toArray, row.isDeterministic(), row.provenance.asString), truples._3.toArray)
     }).unzip3
     val detListsAndProv = resCSV._2.unzip3
     new PythonCSVContainer(resCSV._1.mkString(cols.mkString(", ") + "\n", "\n", ""), detListsAndProv._1.toArray, detListsAndProv._2.toArray, resCSV._3.toArray, detListsAndProv._3.toArray, schViz)
  }
 
 def isWorkflowDeployed(hash:String) : Boolean = {
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
 }
 
 def time[F](anonFunc: => F): (F, Long) = {  
      val tStart = System.nanoTime()
      val anonFuncRet = anonFunc  
      val tEnd = System.nanoTime()
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

//----------------------------------------------------------

trait PythonMimirCallInterface {
	def callToPython(callStr : String) : String
}

class PythonCSVContainer(val csvStr: String, val colsDet: Array[Array[Boolean]], val rowsDet: Array[Boolean], val celReasons:Array[Array[String]], val prov: Array[String], val schema:Map[String, String]){}

