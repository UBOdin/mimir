package mimir;

import java.io._
import java.sql.SQLException

import mimir.ctables.CTPercolator
import mimir.parser._
import mimir.sql._
import mimir.util.{TimeUtils,ExperimentalOptions}
import mimir.algebra.{Operator,Project,ProjectArg,Var,RAException,OperatorUtils}
import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.select.Select
import net.sf.jsqlparser.statement.drop.Drop
import org.rogach.scallop._


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
  var history: List[Operator] = Nil

  def main(args: Array[String]) {
    conf = new MimirConfig(args);

    // Prepare experiments
    ExperimentalOptions.enable(conf.experimental())

    // Set up the database connection(s)
    db = new Database(new JDBCBackend(conf.backend(), conf.dbname()))
    println("Connecting to " + conf.backend() + "://" + conf.dbname() + "...")
    db.backend.open()

    db.initializeDBForMimir();

    // Check for one-off commands
    if(conf.loadTable.get != None){
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
        db.backend.asInstanceOf[JDBCBackend].enableInlining(db)
      }

      if(conf.file.get == None || conf.file() == "-"){
        source = new InputStreamReader(System.in);
        usePrompt = !conf.quiet();
      } else {
        source = new FileReader(conf.file());
        usePrompt = false;
      }

      println("   ... ready")
      eventLoop(source)
    }

    db.backend.close()
    if(!conf.quiet()) { println("\n\nDone.  Exiting."); }
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
          case extend: Extend   => handleExtend(extend)
          case _                => db.update(stmt)
        }

      } catch {
        case e: FileNotFoundException =>
          println(e.getMessage)

        case e: SQLException =>
          println("Error: "+e.getMessage)

        case e: RAException =>
          println("Error: "+e.getMessage)

        case e: Throwable => {
          println("An unknown error occurred...");
          e.printStackTrace()

          // The parser pops the input stream back onto the queue, so
          // the next call to Statement() will throw the same exact 
          // Exception.  To prevent this from happening, reset the parser:
          parser = new MimirJSqlParser(source);
        }
      }
    } while(!done)
  }

  def handleSelect(sel: Select): Unit = {
    val raw = db.sql.convert(sel)
    handleQuery(raw)
  }

  def handleQuery(raw:Operator) = 
  {
    TimeUtils.monitor("QUERY", () => {
      val results = db.query(raw)
      results.open()
      db.dump(results)
      results.close()
      history = raw :: history
    }, println(_))
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

  def handleExtend(extend: Extend): Unit = {
    if(history.isEmpty){ 
      throw new SQLException("EXTEND can not be the first query")
    } else {
      handleQuery(MimirQL.applyExtend(db, history.head, extend))
    }
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