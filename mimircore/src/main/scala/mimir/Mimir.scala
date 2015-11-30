package mimir;

import java.io._

import mimir.ctables.CTPercolator
import mimir.parser._
import mimir.sql._
import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.select.Select
import org.rogach.scallop._;


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

    // Set up the database connection(s)
    db = new Database(conf.dbname(), new JDBCBackend(conf.backend(), conf.dbname()))
    db.backend.open()

    // Check for one-off commands
    if(conf.initDB()){
      println("Initializing Database...");
      db.initializeDBForMimir();
    } else if(conf.loadTable.get != None){
      db.handleLoadTable(conf.loadTable(), conf.loadTable()+".csv");
    } else {
      var source: Reader = null;

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

        if(stmt == null){ done = true; }
        else if(stmt.isInstanceOf[Select]){
          handleSelect(stmt.asInstanceOf[Select]);
        } else if(stmt.isInstanceOf[CreateLens]) {
          db.createLens(stmt.asInstanceOf[CreateLens]);
        } else if(stmt.isInstanceOf[Explain]) {
          handleExplain(stmt.asInstanceOf[Explain]);
        } else {
          db.update(stmt.toString())
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
    val raw = db.convert(explain.getSelectBody())._1
    val optimized = db.optimize(raw)
    val sql = db.convert(optimized)
    println("------ Raw Query ------")
    println(raw)
    println("--- Optimized Query ---")
    println(optimized)
    println("--- SQL Query ---")
    println(sql)
  }

  def handleSelect(sel: Select): Unit = {
    val raw = db.convert(sel)
    val results = db.query(CTPercolator.propagateRowIDs(raw, true))
    results.open()
    db.dump(results)
    results.close()
  }

//  def connectSqlite(filename: String): java.sql.Connection =
//  {
//    Class.forName("org.sqlite.JDBC");
//    java.sql.DriverManager.getConnection("jdbc:sqlite:"+filename);
//  }
//
//  def connectOracle(filename: String): java.sql.Connection =
//  {
//    Methods.getConn()
//  }

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
  val backend = opt[String]("driver", descr = "Which backend database to use? ([sqlite],oracle)",
    default = Some("sqlite"))
  val dbname = opt[String]("db", descr = "Connect to the database with the specified name",
    default = Some("tpch.db"))
  val initDB = toggle("init", default = Some(false))
  val quiet  = toggle("quiet", default = Some(false))
  val file = trailArg[String](required = false)
}