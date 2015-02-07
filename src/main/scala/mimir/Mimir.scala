package mimir;

import java.io._

import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.select.Select
import net.sf.jsqlparser.statement.create.table.CreateTable
import org.rogach.scallop._;
import org.sqlite.JDBC;

import mimir.ctables._;
import mimir.algebra._;
import mimir.sql._;
import mimir.util.TimeUtils;
import mimir.parser._;
import mimir.exec._;

object Mimir {
  
  var conf: MimirConfig = null;
  var backend: Backend = null;
  var iviews: IViewManager = null;
  var usePrompt = true;
  var sqltora: SqlToRA = null
  
  def main(args: Array[String]) {
    conf = new MimirConfig(args);
    
    
    // Set up the database connection(s)
    backend = conf.backend() match {
      case "oracle" => new JDBCBackend(connectOracle());
      case "sqlite" => new JDBCBackend(connectSqlite());
      case x => println("Unsupported backend: "+x); exit(-1);
    }
    
    // Check for one-off commands
    sqltora = new SqlToRA(backend, defineView);
    iviews = new IViewManager(backend, sqltora);
    
    if(conf.initDB()){
      println("Initializing Database...");
      iviews.init();
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
      
      println("Loading IViews...");
      iviews.load();
      println("done");
      
      eventLoop(source);
    }
    println("Done.  Exiting.");
  }
  
  def eventLoop(source: Reader): Unit = {
    var parser = new CCJSqlParser(source);
    var done = false;
    do { 
      try {
        if(usePrompt){ print("\nmimir> "); }
        
        val stmt: Statement = parser.Statement();
        
        if(stmt == null){ done = true; }
        else if(stmt.isInstanceOf[Select]){
          handleSelect(stmt.asInstanceOf[Select]);
        } else if(stmt.isInstanceOf[Analyze]){
          handleAnalyze(stmt.asInstanceOf[Analyze]);
        } else if(stmt.isInstanceOf[CreateIView]) {
          iviews.create(stmt.asInstanceOf[CreateIView]);
        } else if(stmt.isInstanceOf[Explain]) {
          handleExplain(stmt.asInstanceOf[Explain]);
        } else {
          backend.update(stmt.toString())
        }
        
      } catch {
        case e: Throwable => {
          e.printStackTrace()
          println("Command Ignored");
        }
      }
    } while(!done)
  }
  
  def handleExplain(explain: Explain): Unit = {
    val raw = sqltora.convert(explain.getSelectBody());
    println("--- Raw Query ---");
    println(raw.toString());
    println("--- Percolated Query ---");
    println(CTables.percolate(raw).toString);
  }
  
  def handleSelect(sel: Select): Unit = {
    val raw = sqltora.convert(sel);
    val converted = CTables.percolate(raw);
    val results = Compiler.compile(iviews, backend, converted);
    results.open();
    Compiler.dump(results);
    results.close();
  }
  
  def handleAnalyze(request: Analyze): Unit = {
    val raw = sqltora.convert(request.getSelectBody());
    val percolated = CTables.percolate(raw);
    // if(CTAnalysis.isProbabilistic(percolated)){
    //   val analysis = CTAnalysis.analyze(
    //     request.getColumn(), 
    //     percolated
    //   )
    //   // Compute bounds, begin update/resolve loop
    //   println("--- Query ---")
    //   println(request.getSelectBody().toString);
    //   println("--- Expressions ---")
    //   println(analysis.exprs.map ( _.toString ).mkString("\n:: OR ::\n"))
    //   println("Bounds: " + analysis.bounds);
    // } else {
    //   println("The Result is Deterministic")
    // }
  }
  
  def handleLoadTable(targetTable: String, sourceFile: String){
    println("Loading "+sourceFile);
    val sch = backend.getTableSchema(targetTable);
    val keys = sch.map( _._1 )
    val input = new BufferedReader(new FileReader(sourceFile));
    println(""+sch);
    var done = false;
    while(!done){
      val line = input.readLine()
      if(line == null){ done = true; }
      else {
        val data = line.split(",").padTo(keys.length, "");
        backend.update(
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

  
  def connectSqlite(): java.sql.Connection = 
  {
    Class.forName("org.sqlite.JDBC");
    java.sql.DriverManager.getConnection("jdbc:sqlite:debug.db");
  }
  
  def connectOracle(): java.sql.Connection = 
  {
    Methods.getConn()
  }
  
  def defineView(name: String): Option[(Operator)] =
  {
    // System.out.println("Selecting from ..."+name);
    if(iviews == null){ None }
    else {
      //println(iviews.views.toString())
      iviews.views.get(name.toUpperCase()) match {
        case None => None
        case Some(view) => 
          // println("Found: "+name); 
          Some(view.get())
      }
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
  val backend = opt[String]("db", descr = "Which backend database to use? ([sqlite],oracle)", 
                            default = Some("sqlite"))
  val initDB = toggle("init", default = Some(false))
  val quiet  = toggle("quiet", default = Some(false))
  val file = trailArg[String](required = false)
}