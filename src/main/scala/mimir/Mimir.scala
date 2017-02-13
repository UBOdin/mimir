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
object Mimir {

  var conf: MimirConfig = null;
  var db: Database = null;
  var usePrompt = true;

  def main(args: Array[String]) {
    conf = new MimirConfig(args);

    // Prepare experiments
    ExperimentalOptions.enable(conf.experimental())
    
    // Set up the database connection(s)
    db = new Database(new GProMBackend(conf.backend(), conf.dbname(), -1))    
    db.backend.open()

    db.initializeDBForMimir();

    // Check for one-off commands
    /*if(conf.loadTable.get != None){
      db.loadTable(conf.loadTable(), conf.loadTable()+".csv");
    } else if(conf.rebuildBestGuess.get != None){
        db.bestGuessCache.buildCache(
          db.views.getView(
            conf.rebuildBestGuess().toUpperCase
          ).get);
    } else {
      var source: Reader = null;

      conf.precache.foreach( (opt) => opt.split(",").foreach( (table) => { 
        println(s"Precaching... $table")
        db.models.prefetchForOwner(table.toUpperCase)
      }))

      if(ExperimentalOptions.isEnabled("INLINE-VG")){
        db.backend.asInstanceOf[GProMBackend].enableInlining(db)
      }

      if(conf.file.get == None || conf.file() == "-"){
        source = new InputStreamReader(System.in);
        usePrompt = !conf.quiet();
      } else {
        source = new FileReader(conf.file());
        usePrompt = false;
      }

      eventLoop(source)
    }*/

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
     val queryStr = "SELECT RB.INT_COL_B AS B, RC.INT_COL_D AS D FROM TEST_B_RAW RB JOIN TEST_C_RAW RC ON RB.INT_COL_A = RC.INT_COL_D" 
     
     translateOperatorsFromMimirToGProM(("",queryStr));
     translateOperatorsFromGProMToMimir(("", queryStr))
     /*for(i <- 1 to 20){
       translateOperatorsFromMimirToGProMForRewriteFasterThanThroughSQL(("", queryStr +  " WHERE TEST_B_RAW.INT_COL_C = " + i))
     }*/
     
     /*val statements = db.parse(queryStr)
     val testOper2 = db.sql.convert(statements.head.asInstanceOf[Select])
     val operStr2 = testOper2.toString()
     println("---------v Actual Mimir Oper v----------")
     println(operStr2)
     println("---------^ Actual Mimir Oper ^----------")
     
     GProMWrapper.inst.gpromCreateMemContext()
     val gpromNode = GProMWrapper.inst.rewriteQueryToOperatorModel(queryStr+";")
     val nodeStr = GProMWrapper.inst.gpromNodeToString(gpromNode.getPointer())
     GProMWrapper.inst.gpromFreeMemContext()
     println("---------v Actual GProM Oper v----------")
     println(nodeStr)
     println("---------^ Actual GProM Oper ^----------")*/
   
  }
  
  def printOperResults(oper : mimir.algebra.Operator) = {
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

  def translateOperatorsFromMimirToGProM(descAndQuery : (String, String)) = {
         val queryStr = descAndQuery._2 
         val statements = db.parse(queryStr)
         val testOper = db.sql.convert(statements.head.asInstanceOf[Select])
         val gpromNode = OperatorTranslation.mimirOperatorToGProMList(testOper)
         gpromNode.write()
         GProMWrapper.inst.gpromCreateMemContext() 
         val nodeStr = GProMWrapper.inst.gpromNodeToString(gpromNode.getPointer())
         val gpromNode2 = GProMWrapper.inst.rewriteQueryToOperatorModel(queryStr+";")
         val nodeStr2 = GProMWrapper.inst.gpromNodeToString(gpromNode2.getPointer())
         GProMWrapper.inst.gpromFreeMemContext()
         val success = nodeStr.replaceAll("0x[a-zA-Z0-9]+", "").equals(nodeStr2.replaceAll("0x[a-zA-Z0-9]+", ""))
         println("-------------v Mimir Oper v-------------")
         println(testOper)
         println("-------v Translated GProM Oper v--------")
         println(nodeStr)
         println("---------v Actual GProM Oper v----------")
         println(nodeStr2)
         println("----------------^ "+success+" ^----------------")
    }
  
  def translateOperatorsFromGProMToMimir(descAndQuery : (String, String)) =  {
         val queryStr = descAndQuery._2 
         val statements = db.parse(queryStr)
         val testOper2 = db.sql.convert(statements.head.asInstanceOf[Select])
         val operStr2 = testOper2.toString()
         GProMWrapper.inst.gpromCreateMemContext()
         val gpromNode = GProMWrapper.inst.rewriteQueryToOperatorModel(queryStr+";")
         val nodeStr = GProMWrapper.inst.gpromNodeToString(gpromNode.getPointer())
         val testOper = OperatorTranslation.gpromStructureToMimirOperator(0, gpromNode, null)
         val operStr = testOper.toString()
         GProMWrapper.inst.gpromFreeMemContext()
         val success = operStr.equals(operStr2)
         println("---------v Actual GProM Oper v----------")
         println(nodeStr)
         println("-------v Translated Mimir Oper v--------")
         println(operStr)
         println("---------v Actual Mimir Oper v----------")
         println(operStr2)
         println("----------------^ "+success+" ^----------------")
    }
    
    def translateOperatorsFromMimirToGProMToMimir(descAndQuery : (String, String)) =  {
         val queryStr = descAndQuery._2
         val statements = db.parse(queryStr)
         val testOper = db.sql.convert(statements.head.asInstanceOf[Select])
         val operStr = testOper.toString()
         val gpromNode = OperatorTranslation.mimirOperatorToGProMList(testOper)
         gpromNode.write()
         GProMWrapper.inst.gpromCreateMemContext() 
         val nodeStr = GProMWrapper.inst.gpromNodeToString(gpromNode.getPointer())
         val testOper2 = OperatorTranslation.gpromStructureToMimirOperator(0, gpromNode, null)
         val operStr2 = testOper2.toString()
         GProMWrapper.inst.gpromFreeMemContext()
         val success = operStr.equals(operStr2)
         println("---------v Actual Mimir Oper v----------")
         println(operStr)
         println("-------v Translated GProM Oper v--------")
         println(nodeStr)
         println("-------v Translated Mimir Oper v--------")
         println(operStr2)
         println("----------------^ "+success+" ^----------------")
    }
    
    def translateOperatorsFromGProMToMimirToGProM(descAndQuery : (String, String)) =  {
         val queryStr = descAndQuery._2 
         GProMWrapper.inst.gpromCreateMemContext() 
         val gpromNode = GProMWrapper.inst.rewriteQueryToOperatorModel(queryStr+";")
         val testOper = OperatorTranslation.gpromStructureToMimirOperator(0, gpromNode, null)
         val nodeStr = GProMWrapper.inst.gpromNodeToString(gpromNode.getPointer())
         //val statements = db.parse(convert(testOper.toString()))
         //val testOper2 = db.sql.convert(statements.head.asInstanceOf[Select])
         GProMWrapper.inst.gpromFreeMemContext()
         val gpromNode2 = OperatorTranslation.mimirOperatorToGProMList(testOper)
         gpromNode2.write()
         GProMWrapper.inst.gpromCreateMemContext() 
         val nodeStr2 = GProMWrapper.inst.gpromNodeToString(gpromNode2.getPointer())
         GProMWrapper.inst.gpromFreeMemContext()
         val success = nodeStr.replaceAll("0x[a-zA-Z0-9]+", "").equals(nodeStr2.replaceAll("0x[a-zA-Z0-9]+", ""))
         println("---------v Actual GProM Oper v----------")
         println(nodeStr)
         println("-------v Translated Mimir Oper v--------")
         println(testOper)
         println("-------v Translated GProM Oper v--------")
         println(nodeStr2)
         println("----------------^ "+success+" ^----------------")
    }
  
    def translateOperatorsFromMimirToGProMForRewriteFasterThanThroughSQL(descAndQuery : (String, String)) =   {
         val queryStr = descAndQuery._2 
         val statements = db.parse(queryStr)
         val testOper = db.sql.convert(statements.head.asInstanceOf[Select])
         
         val timeForRewriteThroughOperatorTranslation = time {
           val gpromNode = OperatorTranslation.mimirOperatorToGProMList(testOper)
           gpromNode.write()
           GProMWrapper.inst.gpromCreateMemContext() 
           val gpromNode2 = GProMWrapper.inst.provRewriteOperator(gpromNode.getPointer())
           val testOper2 = OperatorTranslation.gpromStructureToMimirOperator(0, gpromNode2, null)
           val operStr = testOper2.toString()
           //val sqlRewritten = db.ra.convert(testOper2)
           GProMWrapper.inst.gpromFreeMemContext()
           operStr
         }
           
         val timeForRewriteThroughSQL = time {
           val sqlToRewrite = db.ra.convert(testOper)
           val sqlRewritten = GProMWrapper.inst.gpromRewriteQuery(sqlToRewrite.toString()+";")
           val statements2 = db.parse(sqlRewritten)
           val testOper2 = db.sql.convert(statements.head.asInstanceOf[Select])
           testOper2.toString()
         }
         val operStr = timeForRewriteThroughOperatorTranslation._1
         val operStr2 = timeForRewriteThroughSQL._1
         val success = operStr.equals(operStr2) &&  (timeForRewriteThroughOperatorTranslation._2 < timeForRewriteThroughSQL._2) 
         
         println("-----------v Translated Mimir Oper v-----------")
         println(operStr)
         println("Time: " + timeForRewriteThroughOperatorTranslation._2)
         println("---------v SQL Translated Mimir Oper v---------")
         println(operStr2)
         println("Time: " + timeForRewriteThroughSQL._2)
         println("----------------^ "+success+" ^----------------")
    }
    
    def time[F](anonFunc: => F): (F, Long) = {  
      val tStart = System.nanoTime()
      val anonFuncRet = anonFunc  
      val tEnd = System.nanoTime()
      (anonFuncRet, tEnd-tStart)
    }
  
  def eventLoop(source: Reader): Unit = {
    var parser = new MimirJSqlParser(source);
    var done = false;
    do {
      try {
        if(usePrompt){ print("\nmimir> "); }

        val stmt: Statement = parser.Statement();

        stmt match {
          case null             => done = true
          case sel:  Select     => handleSelect(sel)
          case expl: Explain    => handleExplain(expl)
          case _                => db.update(stmt)
        }

      } catch {
        case e: Throwable => {
          e.printStackTrace()
          println("Command Ignored");

          // The parser pops the input stream back onto the queue, so
          // the next call to Statement() will throw the same exact 
          // Exception.  To prevent this from happening, reset the parser:
          parser = new MimirJSqlParser(source);
        }
      }
    } while(!done)
  }
  
  
  private def handleStatements(input: String): (List[Statement], List[String]) = {

    val statements = db.parse(input)

    val results = statements.map({
      /*****************************************/           
      case s: Select => {
        try {
         val raw = db.sql.convert(s)
         val results = db.query(raw)
         results.toString()
        } 
      }
      /*****************************************/           
      case s: ProvenanceStatement => {
        try {
         val raw = db.sql.convert(s)
         val results = db.query(raw)
         val data: ListBuffer[(List[String], Boolean)] = new ListBuffer()

         results.open()
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
      }
      /*****************************************/           
      case s: CreateLens =>
        db.update(s)
        "Lens created successfully."
      /*****************************************/           
      case s: Explain => {
        val raw = db.sql.convert(s.getSelectBody());
        val op = db.optimize(raw)
        val res = "------ Raw Query ------\n"+
          raw.toString()+"\n"+
          "--- Optimized Query ---\n"+
          op.toString
        res
      }
      /*****************************************/           
      case s: Statement =>
        db.update(s)
        "Database updated."
    })
    (statements, results)
  }
  

  def handleExplain(explain: Explain): Unit = {
    val raw = db.sql.convert(explain.getSelectBody())
    println("------ Raw Query ------")
    println(raw)
    db.check(raw)
    val optimized = db.optimize(raw)
    println("--- Optimized Query ---")
    println(optimized)
    db.check(optimized)
    println("--- SQL ---")
    try {
      println(db.ra.convert(optimized).toString)
    } catch {
      case e:Throwable =>
        println("Unavailable: "+e.getMessage())
    }
  }

  def handleSelect(sel: Select): Unit = {
    TimeUtils.monitor("QUERY", () => {
      val raw = db.sql.convert(sel)
      val results = db.query(raw)
      results.open()
      db.dump(results)
      results.close()
    }, println(_))
  }

}

class MimirConfig(arguments: Seq[String]) extends ScallopConf(arguments)
{
  //   val start = opt[Long]("start", default = Some(91449149))
  //   val end = opt[Long]("end", default = Some(99041764))
  //   val version_count = toggle("vcount", noshort = true, default = Some(false))
  //   val exclude = opt[Long]("xclude", default = Some(91000000))
  //   val summarize = toggle("summary-create", default = Some(false))
  //   val cleanSummary = toggle("summary-clean", default = Some(false))
  //   val sampleCount = opt[Int]("samples", noshort = true, default = None)
  val loadTable = opt[String]("loadTable", descr = "Don't do anything, just load a CSV file")
  val dbname = opt[String]("db", descr = "Connect to the database with the specified name",
    default = Some("debug.db"))
  val backend = opt[String]("driver", descr = "Which backend database to use? ([sqlite],oracle)",
    default = Some("sqlite"))
  val precache = opt[String]("precache", descr = "Precache one or more lenses")
  val rebuildBestGuess = opt[String]("rebuild-bestguess")  
  val quiet  = toggle("quiet", default = Some(false))
  val file = trailArg[String](required = false)
  val experimental = opt[List[String]]("X", default = Some(List[String]()))
}