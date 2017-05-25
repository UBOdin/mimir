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
import mimir.optimizer.InlineVGTerms
import mimir.exec.Compiler
import mimir.exec.BestGuesser
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
    
    // Set up the database connection(s)
    gp = new GProMBackend(conf.backend(), conf.dbname(), -1)
    db = new Database(gp)    
    db.backend.open()
    gp.metadataLookupPlugin.db = db;
    
    db.initializeDBForMimir();

   if(ExperimentalOptions.isEnabled("INLINE-VG")){
        db.backend.asInstanceOf[GProMBackend].enableInlining(db)
      }
   
   // Prepare GProM-Mimir Translator
   OperatorTranslation.db = db
   
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
     //val queryStr = "SELECT SUM(RB.INT_COL_B) AS SB, COUNT(RB.INT_COL_B) AS CB  FROM TEST_B_RAW RB" 
     /*val queryStr = "SELECT SUM(RB.INT_COL_A) AS RSA, SUM(RB.INT_COL_B), SUM(RB.INT_COL_A + RB.INT_COL_B) AS RSAPB, COUNT(RB.INT_COL_B) FROM TEST_B_RAW RB"
    //val queryStr = "SELECT S.A + S.B AS SAB FROM R AS S"
    
     val allTables = db.getAllTables()
     if(!allTables.contains("R"))
       db.update(new MimirJSqlParser(new StringReader("CREATE TABLE R(A integer, B integer)")).Statement())
     if(!allTables.contains("T"))
       db.update(new MimirJSqlParser(new StringReader("CREATE TABLE T(C integer, D integer)")).Statement())
     
     val memctx = GProMWrapper.inst.gpromCreateMemContext()    
     testDebug = true
     val ConsoleOutputColorMap = Map(true -> (scala.Console.GREEN + "+"), false -> (scala.Console.RED + "-"))
     //print(ConsoleOutputColorMap(translateOperatorsFromMimirToGProM(("",queryStr)))); println(scala.Console.BLACK + " translateOperatorsFromMimirToGProM  " )
     print(ConsoleOutputColorMap(translateOperatorsFromGProMToMimir(("", queryStr)))); println(scala.Console.BLACK + " translateOperatorsFromGProMToMimir  " )
     //print(ConsoleOutputColorMap(translateOperatorsFromMimirToGProMToMimir(("",queryStr)))); println(scala.Console.BLACK + " translateOperatorsFromMimirToGProMToMimir  " )
     //print(ConsoleOutputColorMap(translateOperatorsFromGProMToMimirToGProM(("",queryStr)))); println(scala.Console.BLACK + " translateOperatorsFromGProMToMimirToGProM  " )
     //print(ConsoleOutputColorMap(translateOperatorsFromMimirToGProMForRewriteFasterThanThroughSQL(("",queryStr)))); println(scala.Console.BLACK + " translateOperatorsFromMimirToGProMForRewriteFasterThanThroughSQL  " )
     /*for(i <- 1 to 90){
       translateOperatorsFromMimirToGProMForRewriteFasterThanThroughSQL(("", queryStr ))
     }*/
     GProMWrapper.inst.gpromFreeMemContext(memctx)
     */
     
    //db.update(new MimirJSqlParser(new StringReader("CREATE TABLE R(A integer, B integer)")).Statement())
    //db.update(new MimirJSqlParser(new StringReader("CREATE TABLE T(C integer, D integer)")).Statement())
    //testDebug = true
    //runTests(30) 
    //runTests(1) 
    
    /*val queryString = """SELECT * FROM (SELECT SUBQ_TIME.COMMENT_ARG_0 AS COMMENT_ARG_0, FIRST_INT(SUBQ_TIME.TIME) AS TIME, COUNT(DISTINCT SUBQ_TIME.TIME) AS MIMIR_KR_COUNT_TIME, JSON_GROUP_ARRAY(SUBQ_TIME.TIME) AS MIMIR_KR_HINT_COL_TIME, FIRST_FLOAT(SUBQ_TIME.A1) AS A1, COUNT(DISTINCT SUBQ_TIME.A1) AS MIMIR_KR_COUNT_A1, JSON_GROUP_ARRAY(SUBQ_TIME.A1) AS MIMIR_KR_HINT_COL_A1, FIRST_FLOAT(SUBQ_TIME.A2) AS A2, COUNT(DISTINCT SUBQ_TIME.A2) AS MIMIR_KR_COUNT_A2, JSON_GROUP_ARRAY(SUBQ_TIME.A2) AS MIMIR_KR_HINT_COL_A2, FIRST_FLOAT(SUBQ_TIME.B1) AS B1, COUNT(DISTINCT SUBQ_TIME.B1) AS MIMIR_KR_COUNT_B1, JSON_GROUP_ARRAY(SUBQ_TIME.B1) AS MIMIR_KR_HINT_COL_B1, FIRST_FLOAT(SUBQ_TIME.B2) AS B2, COUNT(DISTINCT SUBQ_TIME.B2) AS MIMIR_KR_COUNT_B2, JSON_GROUP_ARRAY(SUBQ_TIME.B2) AS MIMIR_KR_HINT_COL_B2 FROM (SELECT MIMIRCAST(SUBQ_TIME.TIME, 0) AS TIME, MIMIRCAST(SUBQ_TIME.A1, 1) AS A1, MIMIRCAST(SUBQ_TIME.A2, 1) AS A2, MIMIRCAST(SUBQ_TIME.B1, 1) AS B1, MIMIRCAST(SUBQ_TIME.B2, 1) AS B2, BEST_GUESS_VGTERM('LENS_COMMENT1036885900:TIME10001724279192', 0, SUBQ_TIME.MIMIR_ROWID, (MIMIRCAST(SUBQ_TIME.TIME, 0) / 1000)) AS COMMENT_ARG_0 FROM (SELECT SAMPLE_DATA_SAMPLE2_RAW.TIME AS TIME, SAMPLE_DATA_SAMPLE2_RAW.A1 AS A1, SAMPLE_DATA_SAMPLE2_RAW.A2 AS A2, SAMPLE_DATA_SAMPLE2_RAW.B1 AS B1, SAMPLE_DATA_SAMPLE2_RAW.B2 AS B2, SAMPLE_DATA_SAMPLE2_RAW.ROWID AS MIMIR_ROWID, SAMPLE_DATA_SAMPLE2_RAW.ROWID AS MIMIR_ROWID FROM SAMPLE_DATA_SAMPLE2_RAW AS SAMPLE_DATA_SAMPLE2_RAW) SUBQ_TIME) SUBQ_TIME GROUP BY SUBQ_TIME.COMMENT_ARG_0) SUBQ_COMMENT_ARG_0, (SELECT SAMPLE_DATA_SAMPLE2_RAW.TIME AS TIME_0, SAMPLE_DATA_SAMPLE2_RAW.A1 AS A1_0, SAMPLE_DATA_SAMPLE2_RAW.A2 AS A2_0, SAMPLE_DATA_SAMPLE2_RAW.B1 AS B1_0, SAMPLE_DATA_SAMPLE2_RAW.B2 AS B2_0, SAMPLE_DATA_SAMPLE2_RAW.ROWID AS MIMIR_ROWID FROM SAMPLE_DATA_SAMPLE2_RAW AS SAMPLE_DATA_SAMPLE2_RAW) SUBQ_TIME_0 WHERE (
  		SUBQ_COMMENT_ARG_0.COMMENT_ARG_0 = BEST_GUESS_VGTERM('LENS_COMMENT1036885900:TIME10001724279192', 0, SUBQ_TIME_0.MIMIR_ROWID, (MIMIRCAST(SUBQ_TIME_0.TIME_0, 0) / 1000))
  	)"""
    println("-------v GProM Oper RW v---------")
    println(getQueryResults(queryString))
    println("-------^ GProM Oper RW ^---------")*/
     
    /*val queryStr0 = "SELECT S.A AS P, S.B AS Q FROM R AS S"
    val statements2 = db.parse(queryStr0)
     var testOper4 = db.sql.convert(statements2.head.asInstanceOf[Select])
     var gpnode = GProMWrapper.inst.rewriteQueryToOperatorModel(queryStr0+";")
   val gpnodeStr = GProMWrapper.inst.gpromNodeToString(gpnode.getPointer())
   val mimirnode = OperatorTranslation.gpromStructureToMimirOperator(0, gpnode, null)  
   
     //GProMWrapper.inst.gpromFreeMemContext(memctx)
     println("---------v Actual GProM Oper v----------")
     println(gpnodeStr)
     println("---------^ Actual GProM Oper ^----------")
     println("---------v Trans Mimir Oper v----------")
     println(mimirnode)
     println("---------^ Trans Mimir Oper ^----------")
     println("---------v Actual Mimir Oper v----------")
     println(totallyOptimize(testOper4))
     println("---------^ Actual Mimir Oper ^----------")*/
   
    /*val queryqStr = "SELECT R.A FROM R"
    val querypStr = s"PROVENANCE OF ($queryqStr)"*/
    
    /*var gpnodepStr = GProMWrapper.inst.gpromRewriteQuery(querypStr+";")
   //GProMWrapper.inst.gpromFreeMemContext(memctx)
     println(gpnodepStr)
     println("-------v GProM Oper RW v---------")
     println(getQueryResults(gpnodepStr))
     println("-------^ GProM Oper RW ^---------") */
    
    /*val statements3 = db.parse(queryqStr)
    var testOper8 = db.sql.convert(statements3.head.asInstanceOf[Select])
    val operProv = OperatorTranslation.compileProvenanceWithGProM(testOper8)
    testOper8 = operProv._1 
    println(testOper8)
    testOper8 = BestGuesser.bestGuessQuery(db, testOper8)
    testOper8 = db.backend.specializeQuery(testOper8)
    println("-------v GProM Oper RW v---------")
    println(getQueryResults(testOper8))
    println("-------^ GProM Oper RW ^---------")
     
    var gpnodep = GProMWrapper.inst.rewriteQueryToOperatorModel(querypStr+";")
    var gpnodepStr = GProMWrapper.inst.gpromNodeToString(gpnodep.getPointer())
    println("-------v GProM Oper OMRW v---------")
    println(gpnodepStr)
    println("-------^ GProM Oper OMRW ^---------")
    
    gpnodep = GProMWrapper.inst.provRewriteOperator(gpnodep.getPointer)
    gpnodepStr = GProMWrapper.inst.gpromNodeToString(gpnodep.getPointer())
    println("-------v GProM Oper PRW v---------")
    println(gpnodepStr)
    println("-------^ GProM Oper PRW ^---------")
    
    val statements1 = db.parse(queryqStr)
    var testOper7 = db.sql.convert(statements1.head.asInstanceOf[Select])
    gpnodep = OperatorTranslation.mimirOperatorToGProMList(ProvenanceOf(testOper7))
    gpnodep.write()
    gpnodepStr = GProMWrapper.inst.gpromNodeToString(gpnodep.getPointer())
    println("-------v GProM Oper TRW v---------")
    println(gpnodepStr)
    println("-------^ GProM Oper TRW ^---------")
    
    gpnodep = GProMWrapper.inst.provRewriteOperator(gpnodep.getPointer)
    gpnodepStr = GProMWrapper.inst.gpromNodeToString(gpnodep.getPointer())
    println("-------v GProM Oper TPRW v---------")
    println(gpnodepStr)
    println("-------^ GProM Oper TPRW ^---------")
    
    
    val queryStr = "SELECT R.A FROM R"
    val statements0 = db.parse(queryStr)
    var testOper6 = db.sql.convert(statements0.head.asInstanceOf[Select])
    var gpnode = OperatorTranslation.mimirOperatorToGProMList(ProvenanceOf(testOper6))
    gpnode.write()
    val provGpromNode = GProMWrapper.inst.provRewriteOperator(gpnode.getPointer)
    
    val gpnodeStr = GProMWrapper.inst.gpromNodeToString(provGpromNode.getPointer())
    
     //GProMWrapper.inst.gpromFreeMemContext(memctx)
     println("-------v GProM Oper v---------")
     println(gpnodeStr)
     println("-------^ GProM Oper ^---------")
     
     */
     
     val queryStr = "Select * FROM LENS_MISSING_KEY93961111"//FROM LENS_PICKER2009618197"//LENS_PICKER804228897 "//LENS_PICKER1562291232"//LENS_MISSING_KEY93961111"
     val statements = db.parse(queryStr)
     var testOper2 = db.sql.convert(statements.head.asInstanceOf[Select])
     val query = testOper2//InlineVGTerms(testOper2 )
     testOper2 = OperatorTranslation.optimizeWithGProM(query)
    
    //gp.metadataLookupPlugin.setOper(query)
      
    
    
     /*val inlinedSQL = db.compiler.sqlForBackend(query).toString()
     println("MIMIR: " + inlinedSQL)
     val tmimirres = time{ getQueryResults(inlinedSQL) }
     println(tmimirres._2)
     println("----------------------")
     val sqlRewritten = GProMWrapper.inst.gpromRewriteQuery(inlinedSQL+";")
     println("GPROM: " + sqlRewritten)
     val tgpromres = time{ getQueryResults(sqlRewritten) }
     println(tgpromres._2)*/
    
     /*//testOper2 = db.compiler.optimize(testOper2, db.compiler.standardOptimizations)
     //testOper2 = totallyOptimize(testOper2)
     //val inlinedSQL = db.compiler.sqlForBackend(query).toString()
     val operStr2 = query.toString()
     println("---------v Actual Mimir Oper v----------")
     println(operStr2)
     println("---------^ Actual Mimir Oper ^----------")
     
     println("-------v Optimized Mimir Oper v---------")
     println(testOper2.toString())
     println("-------^ Optimized Mimir Oper ^---------")
     
     val memctx = GProMWrapper.inst.gpromCreateMemContext()
     val memctxq = GProMWrapper.inst.createMemContextName("QUERY_CONTEXT")
     //val gpromNode = GProMWrapper.inst.rewriteQueryToOperatorModel(queryStr+";")
    val gpromNode = OperatorTranslation.mimirOperatorToGProMList(query)
     gpromNode.write()
    /* val nodeStr = GProMWrapper.inst.gpromNodeToString(gpromNode.getPointer())
     
     //GProMWrapper.inst.gpromFreeMemContext(memctx)
     println("---------v Actual GProM Oper v----------")
     println(nodeStr)
     println("---------^ Actual GProM Oper ^----------")
     */
     val optimizedGpromNode = GProMWrapper.inst.optimizeOperatorModel(gpromNode.getPointer)
   
     //val localOp = new GProMNode.ByReference(optimizedGpromNode.getPointer)
     //localOp.write()
     val optNodeStr = GProMWrapper.inst.gpromNodeToString(optimizedGpromNode.getPointer())
     
     //GProMWrapper.inst.gpromFreeMemContext(memctx)
     println("-------v Optimized GProM Oper v---------")
     println(optNodeStr)
     println("-------^ Optimized GProM Oper ^---------")
     
     val testOper3 = OperatorTranslation.gpromStructureToMimirOperator(0, optimizedGpromNode, null)
     val operStr3 = testOper3.toString()
     println("---------v Trans Mimir Oper v----------")
     println(operStr3)
     println("---------^ Trans Mimir Oper ^----------") 
     
     val compiled = BestGuesser(db, testOper3, List())
     
     val gpromOperCompiled = Project(
        testOper3.columnNames.map { name => ProjectArg(name, Var(name)) } ++
        Seq(ProjectArg(Provenance.rowidColnameBase, mimir.algebra.Function(Provenance.mergeRowIdFunction, compiled._5.map( Var(_) ) ))) ++
        compiled._3.map { case (name, expression) => ProjectArg(CTPercolator.mimirColDeterministicColumnPrefix + name, expression) } ++
        Seq( ProjectArg(CTPercolator.mimirRowDeterministicColumnName, compiled._4)),
        compiled._1
      )
     
     val compiledmimir = BestGuesser(db, query, Compiler.standardOptimizations)
     
     val mimirSqlStr = db.compiler.sqlForBackend(compiledmimir._1).toString()
     val gpromSqlStr = db.compiler.sqlForBackend(gpromOperCompiled, List()).toString() 
     
     println(s"mimir op schema: ${compiledmimir._1.schema.mkString(",")}")
     println(s"bprom op schema: ${gpromOperCompiled.schema.mkString(",")}")
     
     for( i <- 1 to 5){
       val mimirRun = time {
         getQueryResults(mimirSqlStr)
       }
       val gpromRun = time {
         getQueryResults(gpromSqlStr)
       }
       
       println(s"""                   ---------Results: ${gpromRun._1.equals(mimirRun._1)}
                   gpromRun: ${gpromRun._2} 
                   mimirRun: ${mimirRun._2}
                   ------------------------""")
       //println(s"gpromRun: ${gpromRun._2}\n${gpromRun._1}\n---------------------\nmimirRun: ${mimirRun._2}\n${mimirRun._1}\n-------------------")
     }
     
     
     /*val optSqlStr = GProMWrapper.inst.operatorModelToSql(optimizedGpromNode.getPointer())
     println("-------v Optimized GProM Sql v---------")
     println(optSqlStr)
     println("-------^ Optimized GProM Sql ^---------")*/*/
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
    val postOptOper = Compiler.optimize(oper)
    val postOpt = postOptOper.toString() 
    if(preOpt.equals(postOpt))
      postOptOper
    else
      totallyOptimize(postOptOper)
  }

  var testDebug = false
  def runTests(runLoops : Int) = {
    val ConsoleOutputColorMap = Map(true -> (scala.Console.GREEN + "+"), false -> (scala.Console.RED + "-"))
    for(i <- 1 to runLoops ){
      val memctx = GProMWrapper.inst.gpromCreateMemContext() 
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
          print(ConsoleOutputColorMap(translateOperatorsFromMimirToGProM(daq._1))); println(scala.Console.BLACK + " translateOperatorsFromMimirToGProM for " + daq._1._1 + " - " + (daq._2+1) + " of " + testSeq.length)
          print(ConsoleOutputColorMap(translateOperatorsFromGProMToMimir(daq._1))); println(scala.Console.BLACK + " translateOperatorsFromGProMToMimir for " + daq._1._1 + " - " + (daq._2+1) + " of " + testSeq.length)
          print(ConsoleOutputColorMap(translateOperatorsFromMimirToGProMToMimir(daq._1))); println(scala.Console.BLACK + " translateOperatorsFromMimirToGProMToMimir for " + daq._1._1 + " - " + (daq._2+1) + " of " + testSeq.length)
          print(ConsoleOutputColorMap(translateOperatorsFromGProMToMimirToGProM(daq._1))); println(scala.Console.BLACK + " translateOperatorsFromGProMToMimirToGProM for " + daq._1._1 + " - " + (daq._2+1) + " of " + testSeq.length)
          print(ConsoleOutputColorMap(translateOperatorsFromMimirToGProMForRewriteFasterThanThroughSQL(daq._1))); println(scala.Console.BLACK + " translateOperatorsFromMimirToGProMForRewriteFasterThanThroughSQL for " + daq._1._1 + " - " + (daq._2+1) + " of " + testSeq.length)
          //print(ConsoleOutputColorMap(mimirMemTest(daq))); println(scala.Console.BLACK + " mimirMemTest for " + daq._1 )
          //print(ConsoleOutputColorMap(gpromMemTest(daq))); println(scala.Console.BLACK + " gpromMemTest for " + daq._1 )
        }
        }
       GProMWrapper.inst.gpromFreeMemContext(memctx)
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
             println("-------------v-- Operators are different but the results are the same --v-------------")
           }
           if(!success || testDebug){
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
             println("-------------v-- Operators are different but the results are the same --v-------------")
           }
           if(!success || testDebug){
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
             println("-------------v-- Operators are different but the results are the same --v-------------")
           }
           if(!success || testDebug){
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
             println("-------------v-- Operators are different but the results are the same --v-------------")
           }
           if(!success || testDebug){
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
           if(!success || testDebug){
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
