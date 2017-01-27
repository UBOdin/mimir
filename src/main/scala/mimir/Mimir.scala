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
    db = new Database(new GProMBackend(conf.backend(), conf.dbname()))
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
   /*val gpromNode = GProMWrapper.inst.rewriteQueryToOperatorModel("SELECT * from TEST_A_RAW;")
    val testOper = OperatorTranslation.gpromStructureToMimirOperator(null, gpromNode)
    for(i <- 1 to 20)
      println("-------")
    println(testOper)
    for(i <- 1 to 20)
      println("-------")*/
    
    //db.backend.execute("Select * from MIMIR_VIEWS")
    
    /*val statements = db.parse("SELECT * from TEST_A_RAW")
    val testOper2 = db.sql.convert(statements.head.asInstanceOf[Select])
    for(i <- 1 to 20)
      println("-------")
    println(testOper2)
    for(i <- 1 to 20)
      println("-------")
      
     val gpromNode = OperatorTranslation.mimirOperatorToGProMStructure(testOper2)
     //val testOper3 = OperatorTranslation.gpromStructureToMimirOperator(null, gpromNode)
     gpromNode.write()*/
    val gpromNode = GProMWrapper.inst.rewriteQueryToOperatorModel("PROVENANCE OF (SELECT * from TEST_A_RAW);")
    //val provReWriteNode = GProMWrapper.inst.provRewriteOperator(gpromNode.getPointer)
    val testOper3 = OperatorTranslation.gpromStructureToMimirOperator(0, null, gpromNode, null)
    for(i <- 1 to 20)
      println("-------")
    println(testOper3)
    for(i <- 1 to 20)
      println("-------")
    
    val results = db.query(testOper3)
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