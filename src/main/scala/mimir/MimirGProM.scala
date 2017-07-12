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
import scala.util.control.Exception.Catch
import org.gprom.jdbc.jna.GProMNode
import mimir.exec.Compiler
import mimir.ctables.CTPercolator
import mimir.provenance.Provenance
import mimir.algebra.ProjectArg
import mimir.algebra.Var
import mimir.algebra.Project
import mimir.algebra.ProvenanceOf

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
  var gp: GProMBackend = null
  var usePrompt = true;
  var pythonMimirCallListeners = Seq[PythonMimirCallInterface]()

  def main(args: Array[String]) {
    conf = new MimirConfig(args);

    // Prepare experiments
    ExperimentalOptions.enable(conf.experimental())
    
   if(false){
      // Set up the database connection(s)
      db = new Database(new JDBCBackend(conf.backend(), conf.dbname()))
      db.backend.open()
    }
    else {
      //Use GProM Backend
      gp = new GProMBackend(conf.backend(), conf.dbname(), -1)
      db = new Database(gp)    
      db.backend.open()
      gp.metadataLookupPlugin.db = db;
    }
    
    db.initializeDBForMimir();

    if(ExperimentalOptions.isEnabled("INLINE-VG")){
      db.backend.asInstanceOf[InlinableBackend].enableInlining(db)
    }
   
   // Prepare GProM-Mimir Translator
   OperatorTranslation.db = db
   
   testing()
    
    db.backend.close()
    if(!conf.quiet()) { println("\n\nDone.  Exiting."); }
  }
  
  def testing() : Unit = {
    //testDebug = true
    //runTests(15) 
    //runTests(1) 
    
    //val sql = "SELECT R.A + R.B AS Z FROM R WHERE R.A = R.B"
    //translateOperatorsFromGProMToMimirToGProM(("testQ",sql))
    
    //val sql = "SELECT R.A, R.B FROM R WHERE R.A = R.B"
    //translateOperatorsFromGProMToMimirToGProM(("testQ",sql))
    
    val sql = "SELECT SUM(INT_COL_B), COUNT(INT_COL_B) FROM TEST_B_RAW"
    translateOperatorsFromMimirToGProM(("testQ",sql))
    
    /*val sql = "SELECT S.A AS P, U.C AS Q FROM R AS S JOIN T AS U ON S.A = U.C"
    var oper = db.sql.convert(db.parse(sql).head.asInstanceOf[Select])
    oper = Provenance.compile(oper)._1
    val sch = db.bestGuessSchema(oper)  
    println(oper)*/
    //translateOperatorsFromGProMToMimir(("testQ",sql))
    
    //val sql = "SELECT SUM(INT_COL_B), COUNT(INT_COL_B) FROM TEST_B_RAW"
    //translateOperatorsFromMimirToGProM(("testQ",sql))
    
    /*val query = "SELECT * FROM LENS_PICKER2009618197"; //airbus eng err repaired data
    val oper = db.sql.convert(db.parse(query).head.asInstanceOf[Select])
    val operResults = printOperResults(oper) 
    
    val query2 = "SELECT * FROM LENS_MISSING_KEY912796204"; //airbus eng err missing groups
    val oper2 = db.sql.convert(db.parse(query2).head.asInstanceOf[Select])
    val operResults2 = printOperResults(oper2) 
    */
        
    /*var query = "PROVENANCE OF (SELECT a FROM r USE PROVENANCE (ROWID))"
    query = GProMWrapper.inst.gpromRewriteQuery(query+";")       
    println(getQueryResults(query))*/
    
    /*println(explainCell("SELECT * FROM LENS_PICKER2009618197", 1, "1420266763").mkString("\n"))
    println("\n\n")
    
    println(explainCell("SELECT * FROM LENS_MISSING_VALUE1914014057", 1, "3").mkString("\n"))
    println("\n\n")
    println(explainCell("SELECT * FROM LENS_MISSING_VALUE1914014057", 1, "2").mkString("\n"))
    */
    
  }
  
  def explainCell(query: String, col:Int, row:String) : Seq[mimir.ctables.Reason] = {
    val timeRes = time {
      println("explainCell: [" + col + "] [ "+ row +" ] [" + query + "]"  ) ;
      val oper = totallyOptimize(db.sql.convert(db.parse(query).head.asInstanceOf[Select]))
      //val compiledOper = db.compiler.compileInline(oper, db.compiler.standardOptimizations)._1
      val cols = oper.columnNames
      //println(s"explaining Cell: [${cols(col)}][$row]")
      //db.explainCell(oper, RowIdPrimitive(row.toString()), cols(col)).toString()
      val provFilteredOper = db.explainer.filterByProvenance(oper,mimir.algebra.RowIdPrimitive(row))
      val subsetReasons = db.explainer.explainSubset(
              provFilteredOper, 
              Seq(cols(col)).toSet, false, false)
      db.explainer.getFocusedReasons(subsetReasons)
    }
    println(s"explainCell Took: ${timeRes._2}")
    timeRes._1.distinct
  }
  
  def getTuple(oper: mimir.algebra.Operator) : Map[String,mimir.algebra.PrimitiveValue] = {
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
        Map[String,mimir.algebra.PrimitiveValue]()
    })
  }
  
  def getQueryResults(oper : mimir.algebra.Operator) : String =  {
    getQueryResults(db.ra.convert(oper).toString())
  }
  
  def getQueryResults(query:String) : String =  {
    val ress = db.backend.execute(query)
    val resmd = ress.getMetaData();
    var i = 1;
    var row = ""
    var resStr = ""
    while(ress.next()){
      i = 1;
      row = ""
      while(i<=resmd.getColumnCount()){
        row += ress.getString(i) + ", ";
        i+=1;
      }
      resStr += row + "\n"
    }
    resStr
  }
 
  def printOperResults(oper : mimir.algebra.Operator) : String =  {
     db.query(oper)(results => { 
      val data: ListBuffer[(List[String], Boolean)] = new ListBuffer()
  
     val cols = results.schema.map(f => f._1)
     println(cols.mkString(", "))
     while(results.hasNext()){
       val row = results.next()
       val list =
        (
          row.provenance.payload.toString ::
            results.schema.zipWithIndex.map( _._2).map( (i) => {
              row(i).toString + (if (!row.isColDeterministic(i)) {"*"} else {""})
            }).toList
        )
        data.append((list, row.isDeterministic()))
      }
      results.close()
      data.foreach(f => println(f._1.mkString(",")))
      data.mkString(",")
      })
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

  var testDebug = false
  var testError = false
  def runTests(runLoops : Int) = {
    val ConsoleOutputColorMap = Map(true -> (scala.Console.GREEN + "+"), false -> (scala.Console.RED + "-"))
    for(i <- 1 to runLoops ){
        
        val testSeq = Seq(
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
          (s"Queries for Tables with Aggregates - run $i",
              "SELECT SUM(INT_COL_B), COUNT(INT_COL_B) FROM TEST_B_RAW"),
          (s"Queries for Aliased Tables with Aggregates - run $i",
              "SELECT SUM(RB.INT_COL_B), COUNT(RB.INT_COL_B) FROM TEST_B_RAW RB"),
          (s"Queries for Aliased Tables with Aggregates with Aliased Attributes - run $i",
              "SELECT SUM(RB.INT_COL_B) AS SB, COUNT(RB.INT_COL_B) AS CB FROM TEST_B_RAW RB"),
          (s"Queries for Aliased Tables with Aggregates with Aliased Attributes Containing Expressions - run $i",
              "SELECT SUM(RB.INT_COL_A + RB.INT_COL_B) AS SAB, COUNT(RB.INT_COL_B) AS CB FROM TEST_B_RAW RB"),
          (s"Queries for Aliased Tables with Aggregates with Expressions of Aggregates with Aliased Attributes Containing Expressions - run $i",
              "SELECT SUM(RB.INT_COL_A + RB.INT_COL_B) + SUM(RB.INT_COL_A + RB.INT_COL_B) AS SAB, COUNT(RB.INT_COL_B) AS CB FROM TEST_B_RAW RB")
          )
          testSeq.zipWithIndex.foreach {
          daq =>  {
            org.gprom.jdbc.jna.GProM_JNA.GC_LOCK.synchronized{
              val memctx = GProMWrapper.inst.gpromCreateMemContext() 
              print(ConsoleOutputColorMap(translateOperatorsFromMimirToGProM(daq._1))); println(scala.Console.BLACK + " translateOperatorsFromMimirToGProM for " + daq._1._1 + " - " + (daq._2+1) + " of " + testSeq.length)
              print(ConsoleOutputColorMap(translateOperatorsFromGProMToMimir(daq._1))); println(scala.Console.BLACK + " translateOperatorsFromGProMToMimir for " + daq._1._1 + " - " + (daq._2+1) + " of " + testSeq.length)
              print(ConsoleOutputColorMap(translateOperatorsFromMimirToGProMToMimir(daq._1))); println(scala.Console.BLACK + " translateOperatorsFromMimirToGProMToMimir for " + daq._1._1 + " - " + (daq._2+1) + " of " + testSeq.length)
              print(ConsoleOutputColorMap(translateOperatorsFromGProMToMimirToGProM(daq._1))); println(scala.Console.BLACK + " translateOperatorsFromGProMToMimirToGProM for " + daq._1._1 + " - " + (daq._2+1) + " of " + testSeq.length)
              print(ConsoleOutputColorMap(translateOperatorsFromMimirToGProMForRewriteFasterThanThroughSQL(daq._1))); println(scala.Console.BLACK + " translateOperatorsFromMimirToGProMForRewriteFasterThanThroughSQL for " + daq._1._1 + " - " + (daq._2+1) + " of " + testSeq.length)
            //print(ConsoleOutputColorMap(mimirMemTest(daq))); println(scala.Console.BLACK + " mimirMemTest for " + daq._1 )
            //print(ConsoleOutputColorMap(gpromMemTest(daq))); println(scala.Console.BLACK + " gpromMemTest for " + daq._1 )
              GProMWrapper.inst.gpromFreeMemContext(memctx)
            }
          }
          }
         
         //scala.sys.runtime.gc()
      
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
         try{
           val statements = db.parse(queryStr)
           val testOper = db.sql.convert(statements.head.asInstanceOf[Select])
           gp.metadataLookupPlugin.setOper(testOper)
           val gpromNode = OperatorTranslation.mimirOperatorToGProMList(testOper)
           gpromNode.write()
           //val memctx = GProMWrapper.inst.gpromCreateMemContext() 
           val nodeStr = GProMWrapper.inst.gpromNodeToString(gpromNode.getPointer())
           val gpromNode2 = GProMWrapper.inst.rewriteQueryToOperatorModel(queryStr+";")
           val nodeStr2 = GProMWrapper.inst.gpromNodeToString(gpromNode2.getPointer())
           //GProMWrapper.inst.gpromFreeMemContext(memctx)
           var success = nodeStr.replaceAll("0x[a-zA-Z0-9]+", "").equals(nodeStr2.replaceAll("0x[a-zA-Z0-9]+", ""))
           if(!success){
             val resQuery = GProMWrapper.inst.gpromOperatorModelToQuery(gpromNode.getPointer)
             success = getQueryResults(resQuery).equals(getQueryResults(queryStr))
             println("\t-------------v-- Operators are different but the results are the same --v-------------")
           }
           if((!success && testError) || testDebug){
             println("-------------v Mimir Oper v-------------")
             println(testOper)
             println("-------v Translated GProM Oper v--------")
             println(nodeStr)
             println("---------v Actual GProM Oper v----------")
             println(nodeStr2)
             println("---------------v Query- v---------------")
             println(queryStr)
             println("----------------^ "+success+" ^----------------")
           }
           success
         }catch {
           case t : Throwable => {
             val success = false
             println("---------------v Query- v---------------")
             println(queryStr)
             println("-------------v Exception- v-------------")
             println(t)
             t.getStackTrace.foreach(f => println(f.toString()))
             println("----------------^ "+success+" ^----------------")
             success
           }
         }
    }
  
  def translateOperatorsFromGProMToMimir(descAndQuery : (String, String)) : Boolean =  {
         val queryStr = descAndQuery._2 
         try{
           val statements = db.parse(queryStr)
           val testOper2 = db.sql.convert(statements.head.asInstanceOf[Select])
           gp.metadataLookupPlugin.setOper(testOper2)
           var operStr2 = testOper2.toString()
           //val memctx = GProMWrapper.inst.gpromCreateMemContext()
           val gpromNode = GProMWrapper.inst.rewriteQueryToOperatorModel(queryStr+";")
           val nodeStr = GProMWrapper.inst.gpromNodeToString(gpromNode.getPointer())
           val testOper = OperatorTranslation.gpromStructureToMimirOperator(0, gpromNode, null)
           gp.metadataLookupPlugin.setOper(testOper)
           
           var operStr = testOper.toString()
           //GProMWrapper.inst.gpromFreeMemContext(memctx)
           var success = operStr.equals(operStr2)
           if(!success){
             operStr2 = totallyOptimize(testOper2).toString()
             operStr = totallyOptimize(testOper).toString()
             success = operStr.equals(operStr2)
           }
           if(!success){
             success = getQueryResults(testOper).equals(getQueryResults(queryStr))
             println("\t-------------v-- Operators are different but the results are the same --v-------------")
           }
           if((!success && testError) || testDebug){
             println("---------v Actual GProM Oper v----------")
             println(nodeStr)
             println("-------v Translated Mimir Oper v--------")
             println(operStr)
             println("---------v Actual Mimir Oper v----------")
             println(operStr2)
             println("---------------v Query- v---------------")
             println(queryStr)
             //println("---------------v Results- v---------------")
             //println(actualOperResultsStr)
             println("----------------^ "+success+" ^----------------")
           }
           success
         }catch {
           case t : Throwable => {
             val success = false
             println("---------------v Query- v---------------")
             println(queryStr)
             println("-------------v Exception- v-------------")
             println(t)
             t.getStackTrace.foreach(f => println(f.toString()))
             println("----------------^ "+success+" ^----------------")
             success
           }
         }
    }
    
    def translateOperatorsFromMimirToGProMToMimir(descAndQuery : (String, String)) : Boolean =  {
         val queryStr = descAndQuery._2
         try{
           val statements = db.parse(queryStr)
           val testOper = db.sql.convert(statements.head.asInstanceOf[Select])
           gp.metadataLookupPlugin.setOper(testOper)
           var operStr = testOper.toString()
           val gpromNode = OperatorTranslation.mimirOperatorToGProMList(testOper)
           gpromNode.write()
           //val memctx = GProMWrapper.inst.gpromCreateMemContext() 
           val nodeStr = GProMWrapper.inst.gpromNodeToString(gpromNode.getPointer())
           val testOper2 = OperatorTranslation.gpromStructureToMimirOperator(0, gpromNode, null)
           var operStr2 = testOper2.toString()
           //GProMWrapper.inst.gpromFreeMemContext(memctx)
           var success = operStr.equals(operStr2)
           if(!success){
             operStr2 = totallyOptimize(testOper2).toString()
             operStr = totallyOptimize(testOper).toString()
             success = operStr.equals(operStr2)
           }
           if(!success){
             success = getQueryResults(testOper2).equals(getQueryResults(queryStr))
             println("\t-------------v-- Operators are different but the results are the same --v-------------")
           }
           if((!success && testError) || testDebug){
             println("---------v Actual Mimir Oper v----------")
             println(operStr)
             println("-------v Translated GProM Oper v--------")
             println(nodeStr)
             println("-------v Translated Mimir Oper v--------")
             println(operStr2)
             println("---------------v Query- v---------------")
             println(queryStr)
             println("----------------^ "+success+" ^----------------")
           }
           success
         }catch {
           case t : Throwable => {
             val success = false
             println("---------------v Query- v---------------")
             println(queryStr)
             println("-------------v Exception- v-------------")
             println(t)
             t.getStackTrace.foreach(f => println(f.toString()))
             println("----------------^ "+success+" ^----------------")
             success
           }
         }
    }
    
    def translateOperatorsFromGProMToMimirToGProM(descAndQuery : (String, String)) : Boolean =  {
         val queryStr = descAndQuery._2 
         try{
           //val memctx = GProMWrapper.inst.gpromCreateMemContext() 
           val gpromNode = GProMWrapper.inst.rewriteQueryToOperatorModel(queryStr+";")
           val testOper = totallyOptimize(OperatorTranslation.gpromStructureToMimirOperator(0, gpromNode, null))
           gp.metadataLookupPlugin.setOper(testOper)
           val nodeStr = GProMWrapper.inst.gpromNodeToString(gpromNode.getPointer())
           //val statements = db.parse(convert(testOper.toString()))
           //val testOper2 = db.sql.convert(statements.head.asInstanceOf[Select])
           //GProMWrapper.inst.gpromFreeMemContext(memctx)
           val gpromNode2 = OperatorTranslation.mimirOperatorToGProMList(testOper)
           gpromNode2.write()
           //val memctx2 = GProMWrapper.inst.gpromCreateMemContext() 
           val nodeStr2 = GProMWrapper.inst.gpromNodeToString(gpromNode2.getPointer())
           //GProMWrapper.inst.gpromFreeMemContext(memctx2)
           var success = nodeStr.replaceAll("0x[a-zA-Z0-9]+", "").equals(nodeStr2.replaceAll("0x[a-zA-Z0-9]+", ""))
           if(!success){
             val resQuery = GProMWrapper.inst.gpromOperatorModelToQuery(gpromNode2.getPointer)
             success = getQueryResults(resQuery).equals(getQueryResults(queryStr))
             println("\t-------------v-- Operators are different but the results are the same --v-------------")
           }
           if((!success && testError) || testDebug){
             println("---------v Actual GProM Oper v----------")
             println(nodeStr)
             println("-------v Translated Mimir Oper v--------")
             println(testOper)
             println("-------v Translated GProM Oper v--------")
             println(nodeStr2)
             println("---------------v Query- v---------------")
             println(queryStr)
             println("----------------^ "+success+" ^----------------")
           }
           success
         }catch {
           case t : Throwable => {
             val success = false
             println("---------------v Query- v---------------")
             println(queryStr)
             println("-------------v Exception- v-------------")
             println(t)
             t.getStackTrace.foreach(f => println(f.toString()))
             println("----------------^ "+success+" ^----------------")
             success
           }
         }
    }
  
    def translateOperatorsFromMimirToGProMForRewriteFasterThanThroughSQL(descAndQuery : (String, String)) : Boolean =   {
         val queryStr = descAndQuery._2 
         try{
           val statements = db.parse(queryStr)
           val testOper = db.sql.convert(statements.head.asInstanceOf[Select])
           
           val timeForRewriteThroughOperatorTranslation = time {
             val gpromNode = OperatorTranslation.mimirOperatorToGProMList(testOper)
             gpromNode.write()
             //val memctx = GProMWrapper.inst.gpromCreateMemContext() 
             val gpromNode2 = GProMWrapper.inst.provRewriteOperator(gpromNode.getPointer())
             //val testOper2 = OperatorTranslation.gpromStructureToMimirOperator(0, gpromNode2, null)
             //val operStr = testOper2.toString()
             //val sqlRewritten = db.ra.convert(testOper2)
             //GProMWrapper.inst.gpromFreeMemContext(memctx)
             ""//operStr
           }
             
           val timeForRewriteThroughSQL = time {
             //val sqlToRewrite = db.ra.convert(testOper)
             //val sqlRewritten = GProMWrapper.inst.gpromRewriteQuery(sqlToRewrite.toString()+";")
             var sqlRewritten = GProMWrapper.inst.gpromRewriteQuery(queryStr+";")
             /*val aggrNoAliasRegex = ("\\s+AS\\s+("+OperatorTranslation.aggOpNames.mkString("\\b", "|\\b", "")+")\\(.+?\\)")
             var i = 0;
             sqlRewritten = sqlRewritten.replaceAll("\\n", " ")
             while(sqlRewritten.matches(".+?" +aggrNoAliasRegex +".+?" )  ){
               sqlRewritten = sqlRewritten.replaceFirst(aggrNoAliasRegex, " AS MIMIR_AGG_" + i)
               i += 1;
             }
             val statements2 = db.parse(sqlRewritten)
             val testOper2 = db.sql.convert(statements.head.asInstanceOf[Select])
             testOper2.toString()*/""
           }
           val operStr = timeForRewriteThroughOperatorTranslation._1
           val operStr2 = timeForRewriteThroughSQL._1
           val success = /*operStr.equals(operStr2) &&*/  (timeForRewriteThroughOperatorTranslation._2 < timeForRewriteThroughSQL._2) 
           if((!success && testError) || testDebug){
             println("-----------v Translated Mimir Oper v-----------")
             println(operStr)
             println("Time: " + timeForRewriteThroughOperatorTranslation._2)
             println("---------v SQL Translated Mimir Oper v---------")
             println(operStr2)
             println("Time: " + timeForRewriteThroughSQL._2)
             println("---------------v Query- v---------------")
             println(queryStr)
             println("----------------^ "+success+" ^----------------")
           }
           success
         }catch {
           case t : Throwable => {
             val success = false
             println("---------------v Query- v---------------")
             println(queryStr)
             println("-------------v Exception- v-------------")
             println(t)
             t.getStackTrace.foreach(f => println(f.toString()))
             println("----------------^ "+success+" ^----------------")
             success
           }
         }
    }
    
    def time[F](anonFunc: => F): (F, Long) = {  
      val tStart = System.nanoTime()
      val anonFuncRet = anonFunc  
      val tEnd = System.nanoTime()
      (anonFuncRet, tEnd-tStart)
    }

}
