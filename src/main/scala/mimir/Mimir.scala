package mimir;

import java.io._

import mimir.algebra.Operator
import mimir.lenses.Lens
import mimir.parser._
import mimir.sql._
import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.select.Select
import org.rogach.scallop._

import scala.collection.mutable


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
    val backend = conf.backend() match {
      case "oracle" => new JDBCBackend(connectOracle());
      case "sqlite" => new JDBCBackend(connectSqlite(conf.dbname()));
      case x => 

      println("Unsupported backend: "+x); exit(-1);
    }
    db = new Database(backend);
    
    // Check for one-off commands
    if(conf.initDB()){
      println("Initializing Database...");
      db.initializeDBForMimir();
    } else if(conf.loadTable.get != None){
      handleLoadTable(conf.loadTable(), conf.loadTable()+".csv");
    } else {
      var source: Reader = null;
      
      if(conf.file.get == None || conf.file() == "-"){ 
        source = new InputStreamReader(System.in);
        usePrompt = !conf.quiet();
      } else {
        source = new FileReader(conf.file());
        usePrompt = false;
      }
      
      eventLoop(source);
    }
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
          db.initCache(stmt.asInstanceOf[Select])
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
    if (explain.getSelectBody() == null) {
      val query = generateExplainQuery(explain)
      val parser = new MimirJSqlParser(new ByteArrayInputStream(query.getBytes()))
      val statement: Statement = parser.Statement()
      handleSelect(statement.asInstanceOf[Select])
    } else {
      val raw = db.convert(explain.getSelectBody())._1;
      println("------ Raw Query ------");
      println(raw.toString());
      println("--- Optimized Query ---");
      println(db.optimize(raw).toString);
    }
  }
  
  def handleSelect(sel: Select): Unit = {
    val raw = db.convert(sel);
    val results = db.query(raw)
    results.open();
    db.dump(results);
    results.close();
  }
  
  def handleLoadTable(targetTable: String, sourceFile: String){
    println("Loading "+sourceFile);
    val sch = db.getTableSchema(targetTable);
    val keys = sch.map( _._1 )
    val input = new BufferedReader(new FileReader(sourceFile));
    println(""+sch);
    var done = false;
    while(!done){
      val line = input.readLine()
      if(line == null){ done = true; }
      else {
        val data = line.split(",").padTo(keys.length, "");
        db.update(
          "INSERT INTO "+targetTable+"("+keys.mkString(", ")+
          ") VALUES ("+
            data.map( _ match {
              case "" => null;
              case x => x
            }).mkString(", ")+
          ")"
        )
      }
    }
  }

  def generateExplainQuery(explain: Explain) : String = {
    val col = explain.getColumn
    val eType = explain.getType
    val idStr = db.getUncertainIds(col) //Some("SANE_R:B:3")
    idStr match {
      case Some(i) => {
        var tabList = mutable.MutableList[String]()
        var rowIdList = mutable.MutableList[String]()
        var colList = mutable.MutableList[String]()
        val ids = i.split("\\|")
        ids.foreach(a => {
          val b = a.split(":")
          tabList += b(0)
          colList += b(1)
          rowIdList += b(2)
        })
        var q = "SELECT " + eType + "(" + colList.head + ") FROM "
        for (i <- rowIdList.indices) {
          val tabName = db.savedTables(i)
          val lens = db.lenses.lensCache.get(tabName.toUpperCase)
            lens match {
            case None =>
            case Some(x) => {
              val op: Operator = x.asInstanceOf[Lens].view
              q += tabName + " WHERE ROWID = " +rowIdList(i) + ";"
              return q
            }
          }
        }
        q
      }
      case None => throw new Exception("No argument by the name " + col + " exists")
    }
  }
  
  def connectSqlite(filename: String): java.sql.Connection = 
  {
    Class.forName("org.sqlite.JDBC");
    java.sql.DriverManager.getConnection("jdbc:sqlite:"+filename);
  }
  
  def connectOracle(): java.sql.Connection = 
  {
    Methods.getConn()
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
  val backend = opt[String]("driver", descr = "Which backend database to use? ([sqlite],oracle)", 
                            default = Some("sqlite"))
  val dbname = opt[String]("db", descr = "Connect to the database with the specified name",
                            default = Some("debug.db"))
  val initDB = toggle("init", default = Some(false))
  val quiet  = toggle("quiet", default = Some(false))
  val file = trailArg[String](required = false)
}