package mimir;

import java.io._

import scala.collection.mutable.ListBuffer

import org.rogach.scallop._

import mimir.parser._
import mimir.sql._
import mimir.util.ExperimentalOptions
import mimir.util.TimeUtils
import net.sf.jsqlparser.statement.Statement
//import net.sf.jsqlparser.statement.provenance.ProvenanceStatement
import net.sf.jsqlparser.statement.select.Select
import mimir.gprom.algebra.OperatorTranslation
import org.gprom.jdbc.jna.GProMWrapper
import py4j.GatewayServer


/**
 * The primary interface to Mimir.  Responsible for:
 * - Parsing and processing command line arguments.
 * - Initializing internal state (Database())
 * - Providing a command-line prompt (if appropriate)
 * - Invoking MimirJSqlParser and dispatching the 
 *   resulting statements to Database()
 *
 * Database() handles all of the logical dispatching,
 * Mimir provides a friendly command-line user 
 * interface on top of Database()
 */
object MimirVizier {

  var conf: MimirConfig = null;
  var db: Database = null;
  var usePrompt = true;
  var pythonMimirCallListeners = Seq[PythonMimirCallInterface]()

  def main(args: Array[String]) {
    conf = new MimirConfig(args);

    // Prepare experiments
    ExperimentalOptions.enable(conf.experimental())
    
    // Set up the database connection(s)
    db = new Database(new GProMBackend(conf.backend(), conf.dbname(), -1))    
    db.backend.open()

    db.initializeDBForMimir();

    runServerForViztrails()
    
    db.backend.close()
    if(!conf.quiet()) { println("\n\nDone.  Exiting."); }
  }
  
  def runServerForViztrails() : Unit = {
    
     val server = new GatewayServer(this, 33388)
     server.addListener(new py4j.GatewayServerListener(){
        def connectionError(connExept : java.lang.Exception) = {
          println("Python GatewayServer connectionError: " + connExept)
        }
  
        def connectionStarted(conn : py4j.Py4JServerConnection) = {
          println("Python GatewayServer connectionStarted: " + conn)
        }
        
        def connectionStopped(conn : py4j.Py4JServerConnection) = {
          println("Python GatewayServer connectionStopped: " + conn)
        }
        
        def serverError(except: java.lang.Exception) = {
          println("Python GatewayServer serverError")
        }
        
        def serverPostShutdown() = {
           println("Python GatewayServer serverPostShutdown")
        }
        
        def serverPreShutdown() = {
           println("Python GatewayServer serverPreShutdown")
        }
        
        def serverStarted() = {
           println("Python GatewayServer serverStarted")
        }
        
        def serverStopped() = {
           println("Python GatewayServer serverStopped")
        }
     })
     server.start()
     
     while(true){
       Thread.sleep(90000)
       
       pythonMimirCallListeners.foreach(listener => {
       
         listener.callToPython("test")
         })
     }
     
    
  }
  
  //-------------------------------------------------
  //Python package defs
  ///////////////////////////////////////////////
  def loadCSV(file : String) : String = {
    println("loadCSV: From Vistrails: [" + file + "]") ;
    val csvFile = new File(file)
    val tableName = (csvFile.getName().split("\\.")(0) + "_RAW").toUpperCase
    if(db.getAllTables().contains(tableName)){
      println("loadCSV: From Vistrails: Table Already Exists: " + tableName)
    }
    else{
      db.loadTable(csvFile)
    }
    return tableName 
  }
  
  def createLense(input : Any, params : Seq[String], _type : String) : String = {
    println("createLense: From Vistrails: [" + input + "] [" + params.mkString(",") + "] [" + _type + "]"  ) ;
    val paramsStr = params.mkString(",")
    val lenseName = "LENSE_" + ((input.toString() + _type + paramsStr).hashCode().toString().replace("-", "") )
    db.getView(lenseName) match {
      case None => {
        val query = s"CREATE LENS ${lenseName} AS SELECT * FROM ${input} WITH ${_type}(${paramsStr})"  
        println("createLense: query: " + query)
        db.update(db.parse(query).head)
      }
      case Some(_) => {
        println("createLense: From Vistrails: Lense already exists" + lenseName)
      }
    }
    lenseName
  }
  
  def vistrailsQueryMimir(query : String) : String = {
    val oper = db.sql.convert(db.parse(query).head.asInstanceOf[Select])
    operCSVResultsDeterminism(oper)
  }
  
  def registerPythonMimirCallListener(listener : PythonMimirCallInterface) = {
    println("registerPythonMimirCallListener: From Vistrails: ") ;
    pythonMimirCallListeners = pythonMimirCallListeners.union(Seq(listener))
  }
  
  def getAvailableLenses() : String = {
    db.lenses.lensTypes.keySet.toSeq.mkString(",")
  }
  
  def operCSVResults(oper : mimir.algebra.Operator) : String =  {
    val results = db.query(oper)
    var resCSV = ""
    results.open()
    val cols = results.schema.map(f => f._1)
    resCSV += cols.mkString(", ") + "\n"
    while(results.getNext()){
      resCSV += results.currentRow().mkString(", ") + "\n"   
    }
    results.close()
    resCSV 
  }
  //----------------------------------------------------------
  
  def operCSVResultsDeterminism(oper : mimir.algebra.Operator) : String =  {
     val results = db.query(oper)
     var resCSV = ""
     results.open()
     val cols = results.schema.map(f => f._1)
     resCSV += cols.mkString(", ") + "\n"
     while(results.getNext()){
       val list = results.schema.zipWithIndex.map( _._2).map( (i) => {
         results(i).toString + (if (!results.deterministicCol(i)) {"<?-*-?>"} else {""}) + (if (!results.deterministicRow()) {"<?---?>"} else {""}) 
       }) 
       resCSV += list.mkString(", ") + "\n"
      }
      results.close()
      resCSV
  }
  
}

trait PythonMimirCallInterface {
	def callToPython(callStr : String) : String
}
