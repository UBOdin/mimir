package mimir;

import java.io._
import java.sql.SQLException

import mimir.ctables.CTPercolator
import mimir.parser._
import mimir.sql._
import mimir.util.{TimeUtils,ExperimentalOptions}
import mimir.algebra.{Project,ProjectArg,Var}
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

  def main(args: Array[String]) {
    conf = new MimirConfig(args);

    // Prepare experiments
    ExperimentalOptions.enable(conf.experimental())

    // Set up the database connection(s)
    db = new Database(conf.dbname(), new JDBCBackend(conf.backend(), conf.dbname()))
    db.backend.open()

    // Check for one-off commands
    if(conf.initDB()){
      println("Initializing Database...");
      db.initializeDBForMimir();
    } else if(conf.loadTable.get != None){
      db.loadTable(conf.loadTable(), conf.loadTable()+".csv");
    } else if(conf.rebuildBestGuess.get != None){
        val lens = db.lenses.load(conf.rebuildBestGuess().toUpperCase).get
        db.bestGuessCache.buildCache(lens);
    } else {
      var source: Reader = null;

      conf.precache.foreach( (opt) => opt.split(",").foreach( (table) => { 
        println(s"Precaching... $table")
        db.lenses.load(table.toUpperCase)
      }))

      if(conf.file.get == None || conf.file() == "-"){
        source = new InputStreamReader(System.in);
        usePrompt = !conf.quiet();
      } else {
        source = new FileReader(conf.file());
        usePrompt = false;
      }

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
          case crel: CreateLens => db.createLens(crel)
          case expl: Explain    => handleExplain(expl)
          case drop: Drop       => handleDrop(drop)
          case _                => db.backend.update(stmt.toString())
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
    TimeUtils.monitor("QUERY", _ => {
      val raw = db.sql.convert(sel)
      val results = db.query(raw)
      results.open()
      db.dump(results)
      results.close()
    })
  }

  def handleDrop(drop: Drop): Unit = {
    drop.getType().toUpperCase match {
      case "TABLE" | "INDEX" => 
        db.backend.update(drop.toString());

      case "VIEW" =>
        throw new SQLException("Views not supported yet")

      case "LENS" =>
        db.lenses.drop(drop.getName())

      case _ =>
        throw new SQLException("Invalid drop type '"+drop.getType()+"'")

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
  val initDB = toggle("init", default = Some(false))
  val quiet  = toggle("quiet", default = Some(false))
  val file = trailArg[String](required = false)
  val experimental = opt[List[String]]("X", default = Some(List[String]()))
}