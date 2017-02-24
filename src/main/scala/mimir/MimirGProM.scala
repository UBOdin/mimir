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
object MimirGProM {

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

   
    mikeMessingAround()
    
    db.backend.close()
    if(!conf.quiet()) { println("\n\nDone.  Exiting."); }
  }
  
  def mikeMessingAround() : Unit = {
    //val dbtables = db.backend.getAllTables()
    //println( dbtables.mkString("\n") )
    //println( db.backend.getTableSchema(dbtables(0) ))
    //val res = handleStatements("PROVENANCE OF (Select * from TESTDATA_RAW)")
    //val ress = db.backend.execute("PROVENANCE WITH TABLE TEST_A_RAW OF (SELECT TEST_B_RAW.* from TEST_A_RAW JOIN TEST_B_RAW ON TEST_A_RAW.ROWID = TEST_B_RAW.ROWID );")
    /*val ress = db.backend.execute("PROVENANCE OF ( SELECT * from TEST_A_RAW );")
    val resmd = ress.getMetaData();
    var i = 1;
    var row = ""
    while(i<=resmd.getColumnCount()){
      row += resmd.getColumnName(i) + ", ";
      i+=1;
    }
    println(row)
    while(ress.next()){
      i = 1;
      row = ""
      while(i<=resmd.getColumnCount()){
        row += ress.getString(i) + ", ";
        i+=1;
      }
      println(row)
    }*/
    //db.backend.execute("PROVENANCE OF (Select * from TEST_A_RAW)")
    //db.loadTable("/Users/michaelbrachmann/Documents/test_a.mcsv")
    //db.loadTable("/Users/michaelbrachmann/Documents/test_b.mcsv")
  
      
    //val queryStr = "PROVENANCE OF (SELECT * from TEST_A_RAW R WHERE R.INSANE = 'true' AND (R.CITY = 'Utmeica' OR R.CITY = 'Ruminlow'))"
    //val queryStr = "PROVENANCE OF (SELECT * from TEST_A_RAW)"
    //val queryStr = "SELECT * from TEST_A_RAW R WHERE R.INSANE = 'true' AND (R.CITY = 'Utmeica' OR R.CITY = 'Ruminlow')"
     //val queryStr = "SELECT T.INT_COL_A + T.INT_COL_B AS AB, T.INT_COL_B + T.INT_COL_C FROM TEST_B_RAW AS T" 
     //val queryStr = "SELECT RB.INT_COL_B AS B, RC.INT_COL_D AS D FROM TEST_B_RAW RB JOIN TEST_C_RAW RC ON RB.INT_COL_A = RC.INT_COL_D" 
     /*val queryStr = "SELECT SUM(RB.INT_COL_B) AS SB, COUNT(RB.INT_COL_B) AS CB  FROM TEST_B_RAW RB" 
     
     val memctx = GProMWrapper.inst.gpromCreateMemContext()    
     //testDebug = true
     val ConsoleOutputColorMap = Map(true -> (scala.Console.GREEN + "+"), false -> (scala.Console.RED + "-"))
     print(ConsoleOutputColorMap(translateOperatorsFromMimirToGProM(("",queryStr)))); println(scala.Console.BLACK + " translateOperatorsFromMimirToGProM  " )
     print(ConsoleOutputColorMap(translateOperatorsFromGProMToMimir(("", queryStr)))); println(scala.Console.BLACK + " translateOperatorsFromGProMToMimir  " )
     print(ConsoleOutputColorMap(translateOperatorsFromMimirToGProMToMimir(("",queryStr)))); println(scala.Console.BLACK + " translateOperatorsFromMimirToGProMToMimir  " )
     print(ConsoleOutputColorMap(translateOperatorsFromGProMToMimirToGProM(("",queryStr)))); println(scala.Console.BLACK + " translateOperatorsFromGProMToMimirToGProM  " )
     print(ConsoleOutputColorMap(translateOperatorsFromMimirToGProMForRewriteFasterThanThroughSQL(("",queryStr)))); println(scala.Console.BLACK + " translateOperatorsFromMimirToGProMForRewriteFasterThanThroughSQL  " )
     for(i <- 1 to 90){
       translateOperatorsFromMimirToGProMForRewriteFasterThanThroughSQL(("", queryStr ))
     }
     GProMWrapper.inst.gpromFreeMemContext(memctx)*/
     
     
    //db.update(new MimirJSqlParser(new StringReader("CREATE TABLE R(A integer, B integer)")).Statement())
    //db.update(new MimirJSqlParser(new StringReader("CREATE TABLE T(C integer, D integer)")).Statement())
    runTests(10) 
   
    
     /*val statements = db.parse(queryStr)
     var testOper2 = db.sql.convert(statements.head.asInstanceOf[Select])
     testOper2 = db.compiler.optimize(testOper2, db.compiler.standardOptimizations)
     val operStr2 = testOper2.toString()
     println("---------v Actual Mimir Oper v----------")
     println(operStr2)
     println("---------^ Actual Mimir Oper ^----------")
     
     val memctx = GProMWrapper.inst.gpromCreateMemContext()
     val gpromNode = GProMWrapper.inst.rewriteQueryToOperatorModel(queryStr+";")
     val nodeStr = GProMWrapper.inst.gpromNodeToString(gpromNode.getPointer())
     GProMWrapper.inst.gpromFreeMemContext(memctx)
     println("---------v Actual GProM Oper v----------")
     println(nodeStr)
     println("---------^ Actual GProM Oper ^----------")*/
   
  }
  
 
  def printOperResults(oper : mimir.algebra.Operator) : String =  {
     val results = db.query(oper)
      val data: ListBuffer[(List[String], Boolean)] = new ListBuffer()
  
     results.open()
     val cols = results.schema.map(f => f._1)
     println(cols.mkString(", "))
     while(results.getNext()){
       val list =
        (
          results.provenanceToken().payload.toString ::
            results.schema.zipWithIndex.map( _._2).map( (i) => {
              results(i).toString + (if (!results.deterministicCol(i)) {"*"} else {""})
            }).toList
        )
        data.append((list, results.deterministicRow()))
      }
      results.close()
      data.foreach(f => println(f._1.mkString(",")))
      data.mkString(",")
  }

  var testDebug = false
  def runTests(runLoops : Int) = {
    val ConsoleOutputColorMap = Map(true -> (scala.Console.GREEN + "+"), false -> (scala.Console.RED + "-"))
    for(i <- 1 to runLoops ){
      val memctx = GProMWrapper.inst.gpromCreateMemContext() 
      Seq(
        (s"Queries for Tables - run $i", 
            "SELECT R.A, R.B FROM R" ), 
        (s"Queries for Aliased Tables - run $i", 
            "SELECT S.A, S.B FROM R AS S" ), 
        (s"Queries for Tables with Aliased Attributes - run $i", 
            "SELECT R.A AS P, R.B AS Q FROM R"), 
        (s"Queries for Aliased Tables with Aliased Attributes- run $i", 
            "SELECT S.A AS P, S.B AS Q FROM R AS S"),
        (s"Queries for Tables with Epression Attrinutes - run $i",
            "SELECT R.A + R.B AS Z FROM R"),
        (s"Queries for Aliased Tables with Epression Attrinutes - run $i",
            "SELECT S.A + S.B AS Z FROM R AS S"),
        (s"Queries for Tables with Selection - run $i", 
            "SELECT R.A, R.B FROM R WHERE R.A = R.B" ), 
        (s"Queries for Aliased Tables with Selection - run $i", 
            "SELECT S.A, S.B FROM R AS S WHERE S.A = S.B" ), 
        (s"Queries for Tables with Aliased Attributes with Selection - run $i", 
            "SELECT R.A AS P, R.B AS Q FROM R WHERE R.A = R.B"), 
        (s"Queries for Aliased Tables with Aliased Attributes with Selection- run $i", 
            "SELECT S.A AS P, S.B AS Q FROM R AS S WHERE S.A = S.B"), 
        (s"Queries for Tables with Epression Attrinutes with Selection- run $i",
            "SELECT R.A + R.B AS Z FROM R WHERE R.A = R.B"),
        (s"Queries for Aliased Tables with Epression Attrinutes with Selection- run $i",
            "SELECT S.A + S.B AS Z FROM R AS S WHERE S.A = S.B"),
        (s"Queries for Aliased Tables with Joins with Aliased Attributes - run $i", 
            "SELECT S.A AS P, U.C AS Q FROM R AS S JOIN T AS U ON S.A = U.C"),
        (s"Queries for Aliased Tables with Aggragates with Aliased Attributes - run $i",
            "SELECT SUM(RB.INT_COL_B) AS SB, COUNT(RB.INT_COL_B) AS CB FROM TEST_B_RAW RB")
        ).foreach {
        daq =>  {
          print(ConsoleOutputColorMap(translateOperatorsFromMimirToGProM(daq))); println(scala.Console.BLACK + " translateOperatorsFromMimirToGProM for " + daq._1 )
          print(ConsoleOutputColorMap(translateOperatorsFromGProMToMimir(daq))); println(scala.Console.BLACK + " translateOperatorsFromGProMToMimir for " + daq._1 )
          print(ConsoleOutputColorMap(translateOperatorsFromMimirToGProMToMimir(daq))); println(scala.Console.BLACK + " translateOperatorsFromMimirToGProMToMimir for " + daq._1 )
          print(ConsoleOutputColorMap(translateOperatorsFromGProMToMimirToGProM(daq))); println(scala.Console.BLACK + " translateOperatorsFromGProMToMimirToGProM for " + daq._1 )
          print(ConsoleOutputColorMap(translateOperatorsFromMimirToGProMForRewriteFasterThanThroughSQL(daq))); println(scala.Console.BLACK + " translateOperatorsFromMimirToGProMForRewriteFasterThanThroughSQL for " + daq._1 )
          //print(ConsoleOutputColorMap(mimirMemTest(daq))); println(scala.Console.BLACK + " mimirMemTest for " + daq._1 )
          //print(ConsoleOutputColorMap(gpromMemTest(daq))); println(scala.Console.BLACK + " gpromMemTest for " + daq._1 )
        }
        }
       GProMWrapper.inst.gpromFreeMemContext(memctx)
       scala.sys.runtime.gc()
     } 
  }
  
  def mimirMemTest(descAndQuery : (String, String)) : Boolean = {
         val queryStr = descAndQuery._2 
         val statements = db.parse(queryStr)
         val testOper = db.sql.convert(statements.head.asInstanceOf[Select])
         true
  }
  
  def gpromMemTest(descAndQuery : (String, String)) : Boolean = {
         val queryStr = descAndQuery._2 
         val sqlRewritten = GProMWrapper.inst.gpromRewriteQuery(queryStr+";")
         true
  }
  
  def translateOperatorsFromMimirToGProM(descAndQuery : (String, String)) : Boolean = {
         val queryStr = descAndQuery._2 
         val statements = db.parse(queryStr)
         val testOper = db.sql.convert(statements.head.asInstanceOf[Select])
         val gpromNode = OperatorTranslation.mimirOperatorToGProMList(testOper)
         gpromNode.write()
         //val memctx = GProMWrapper.inst.gpromCreateMemContext() 
         val nodeStr = GProMWrapper.inst.gpromNodeToString(gpromNode.getPointer())
         val gpromNode2 = GProMWrapper.inst.rewriteQueryToOperatorModel(queryStr+";")
         val nodeStr2 = GProMWrapper.inst.gpromNodeToString(gpromNode2.getPointer())
         //GProMWrapper.inst.gpromFreeMemContext(memctx)
         val success = nodeStr.replaceAll("0x[a-zA-Z0-9]+", "").equals(nodeStr2.replaceAll("0x[a-zA-Z0-9]+", ""))
         if(!success || testDebug){
           println("-------------v Mimir Oper v-------------")
           println(testOper)
           println("-------v Translated GProM Oper v--------")
           println(nodeStr)
           println("---------v Actual GProM Oper v----------")
           println(nodeStr2)
           println("----------------^ "+success+" ^----------------")
         }
         success
    }
  
  def translateOperatorsFromGProMToMimir(descAndQuery : (String, String)) : Boolean =  {
         val queryStr = descAndQuery._2 
         val statements = db.parse(queryStr)
         val testOper2 = db.sql.convert(statements.head.asInstanceOf[Select])
         val operStr2 = testOper2.toString()
         //val memctx = GProMWrapper.inst.gpromCreateMemContext()
         val gpromNode = GProMWrapper.inst.rewriteQueryToOperatorModel(queryStr+";")
         val nodeStr = GProMWrapper.inst.gpromNodeToString(gpromNode.getPointer())
         val testOper = OperatorTranslation.gpromStructureToMimirOperator(0, gpromNode, null)
         val operStr = testOper.toString()
         //GProMWrapper.inst.gpromFreeMemContext(memctx)
         val success = operStr.equals(operStr2)
         if(!success || testDebug){
           println("---------v Actual GProM Oper v----------")
           println(nodeStr)
           println("-------v Translated Mimir Oper v--------")
           println(operStr)
           println("---------v Actual Mimir Oper v----------")
           println(operStr2)
           println("----------------^ "+success+" ^----------------")
         }
         success
    }
    
    def translateOperatorsFromMimirToGProMToMimir(descAndQuery : (String, String)) : Boolean =  {
         val queryStr = descAndQuery._2
         val statements = db.parse(queryStr)
         val testOper = db.sql.convert(statements.head.asInstanceOf[Select])
         val operStr = testOper.toString()
         val gpromNode = OperatorTranslation.mimirOperatorToGProMList(testOper)
         gpromNode.write()
         //val memctx = GProMWrapper.inst.gpromCreateMemContext() 
         val nodeStr = GProMWrapper.inst.gpromNodeToString(gpromNode.getPointer())
         val testOper2 = OperatorTranslation.gpromStructureToMimirOperator(0, gpromNode, null)
         val operStr2 = testOper2.toString()
         //GProMWrapper.inst.gpromFreeMemContext(memctx)
         val success = operStr.equals(operStr2)
         if(!success || testDebug){
           println("---------v Actual Mimir Oper v----------")
           println(operStr)
           println("-------v Translated GProM Oper v--------")
           println(nodeStr)
           println("-------v Translated Mimir Oper v--------")
           println(operStr2)
           println("----------------^ "+success+" ^----------------")
         }
         success
    }
    
    def translateOperatorsFromGProMToMimirToGProM(descAndQuery : (String, String)) : Boolean =  {
         val queryStr = descAndQuery._2 
         //val memctx = GProMWrapper.inst.gpromCreateMemContext() 
         val gpromNode = GProMWrapper.inst.rewriteQueryToOperatorModel(queryStr+";")
         val testOper = OperatorTranslation.gpromStructureToMimirOperator(0, gpromNode, null)
         val nodeStr = GProMWrapper.inst.gpromNodeToString(gpromNode.getPointer())
         //val statements = db.parse(convert(testOper.toString()))
         //val testOper2 = db.sql.convert(statements.head.asInstanceOf[Select])
         //GProMWrapper.inst.gpromFreeMemContext(memctx)
         val gpromNode2 = OperatorTranslation.mimirOperatorToGProMList(testOper)
         gpromNode2.write()
         //val memctx2 = GProMWrapper.inst.gpromCreateMemContext() 
         val nodeStr2 = GProMWrapper.inst.gpromNodeToString(gpromNode2.getPointer())
         //GProMWrapper.inst.gpromFreeMemContext(memctx2)
         val success = nodeStr.replaceAll("0x[a-zA-Z0-9]+", "").equals(nodeStr2.replaceAll("0x[a-zA-Z0-9]+", ""))
         if(!success || testDebug){
           println("---------v Actual GProM Oper v----------")
           println(nodeStr)
           println("-------v Translated Mimir Oper v--------")
           println(testOper)
           println("-------v Translated GProM Oper v--------")
           println(nodeStr2)
           println("----------------^ "+success+" ^----------------")
         }
         success
    }
  
    def translateOperatorsFromMimirToGProMForRewriteFasterThanThroughSQL(descAndQuery : (String, String)) : Boolean =   {
         val queryStr = descAndQuery._2 
         val statements = db.parse(queryStr)
         val testOper = db.sql.convert(statements.head.asInstanceOf[Select])
         
         val timeForRewriteThroughOperatorTranslation = time {
           val gpromNode = OperatorTranslation.mimirOperatorToGProMList(testOper)
           gpromNode.write()
           //val memctx = GProMWrapper.inst.gpromCreateMemContext() 
           val gpromNode2 = GProMWrapper.inst.provRewriteOperator(gpromNode.getPointer())
           val testOper2 = OperatorTranslation.gpromStructureToMimirOperator(0, gpromNode2, null)
           val operStr = testOper2.toString()
           //val sqlRewritten = db.ra.convert(testOper2)
           //GProMWrapper.inst.gpromFreeMemContext(memctx)
           operStr
         }
           
         val timeForRewriteThroughSQL = time {
           //val sqlToRewrite = db.ra.convert(testOper)
           //val sqlRewritten = GProMWrapper.inst.gpromRewriteQuery(sqlToRewrite.toString()+";")
           val sqlRewritten = GProMWrapper.inst.gpromRewriteQuery(queryStr+";")
           val statements2 = db.parse(sqlRewritten)
           val testOper2 = db.sql.convert(statements.head.asInstanceOf[Select])
           testOper2.toString()
         }
         val operStr = timeForRewriteThroughOperatorTranslation._1
         val operStr2 = timeForRewriteThroughSQL._1
         val success = operStr.equals(operStr2) &&  (timeForRewriteThroughOperatorTranslation._2 < timeForRewriteThroughSQL._2) 
         if(!success || testDebug){
           println("-----------v Translated Mimir Oper v-----------")
           println(operStr)
           println("Time: " + timeForRewriteThroughOperatorTranslation._2)
           println("---------v SQL Translated Mimir Oper v---------")
           println(operStr2)
           println("Time: " + timeForRewriteThroughSQL._2)
           println("----------------^ "+success+" ^----------------")
         }
         success
    }
    
    def time[F](anonFunc: => F): (F, Long) = {  
      val tStart = System.nanoTime()
      val anonFuncRet = anonFunc  
      val tEnd = System.nanoTime()
      (anonFuncRet, tEnd-tStart)
    }

}

