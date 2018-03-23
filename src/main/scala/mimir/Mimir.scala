package mimir;

import java.io._
import java.sql.SQLException

import mimir.ctables._
import mimir.parser._
import mimir.sql._
import mimir.util.{Timer,ExperimentalOptions,LineReaderInputSource,PythonProcess,SqlUtils}
import mimir.algebra._
import mimir.statistics.DetectSeries
import mimir.plot.Plot
import mimir.exec.{OutputFormat,DefaultOutputFormat,PrettyOutputFormat}
import mimir.exec.result.JDBCResultIterator
import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.select.{FromItem, PlainSelect, Select, SelectBody} 
import net.sf.jsqlparser.statement.drop.Drop
import org.jline.terminal.{Terminal,TerminalBuilder}
import org.slf4j.{LoggerFactory}
import org.rogach.scallop._

import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.collection.JavaConverters._
import mimir.algebra.spark.OperatorTranslation

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
object Mimir extends LazyLogging {

  var conf: MimirConfig = null;
  var db: Database = null;
  lazy val terminal: Terminal = TerminalBuilder.terminal()
  var output: OutputFormat = DefaultOutputFormat

  def main(args: Array[String]) = 
  {
    conf = new MimirConfig(args);

    // Prepare experiments
    ExperimentalOptions.enable(conf.experimental())

   
    // Set up the database connection(s)
    db = new Database(new SparkBackend(), new JDBCMetadataBackend(conf.backend(), conf.dbname()))
    if(!conf.quiet()){
      output.print("Connecting to " + conf.backend() + "://" + conf.dbname() + "...")
    }
    db.metadataBackend.open()
    db.backend.open()
    OperatorTranslation.db = db
    
    db.initializeDBForMimir();

    // Check for one-off commands
    if(conf.loadTable.get != None){
      db.loadTable(conf.loadTable(), conf.loadTable()+".csv");
    } else {
      var source: Reader = null;
      var prompt: (() => Unit) = { () =>  }

      conf.precache.foreach( (opt) => opt.split(",").foreach( (table) => { 
        output.print(s"Precaching... $table")
        db.models.prefetchForOwner(table.toUpperCase)
      }))

      if(!ExperimentalOptions.isEnabled("NO-INLINE-VG")){
        db.metadataBackend.asInstanceOf[InlinableBackend].enableInlining(db)
      }

      if(conf.file.get == None || conf.file() == "-"){
        if(!ExperimentalOptions.isEnabled("SIMPLE-TERM")){
          source = new LineReaderInputSource(terminal)
          output = new PrettyOutputFormat(terminal)
        } else {
          source = new InputStreamReader(System.in)
          output = DefaultOutputFormat
          prompt = () => { System.out.print("\nmimir> "); System.out.flush(); }
        }
      } else {
        source = new FileReader(conf.file())
        output = DefaultOutputFormat
      }

      if(!conf.quiet()){
        output.print("   ... ready")
      }
      eventLoop(source, prompt)
    }

    db.backend.close()
    if(!conf.quiet()) { output.print("\n\nDone.  Exiting."); }
  }

  def eventLoop(source: Reader, prompt: (() => Unit)): Unit =
  {
    var parser = new MimirJSqlParser(source);
    var done = false;
    do {
      try {
        prompt()
        val stmt: Statement = parser.Statement();

        stmt match {
          case null             => done = true
          case sel:  Select     => handleSelect(sel)
          case expl: Explain    => handleExplain(expl)
          case pragma: Pragma   => handlePragma(pragma)
          case analyze: Analyze => handleAnalyze(analyze)
          case plot: DrawPlot   => Plot.plot(plot, db, output)
          case dir: DirectQuery => handleDirectQuery(dir)
          case _                => db.update(stmt)
        }

      } catch {
        case e: FileNotFoundException =>
          output.print(e.getMessage)

        case e: SQLException =>
          output.print("Error: "+e.getMessage)
          logger.debug(e.getMessage + "\n" + e.getStackTrace.map(_.toString).mkString("\n"))

        case e: RAException =>
          output.print("Error: "+e.getMessage)
          logger.debug(e.getMessage + "\n" + e.getStackTrace.map(_.toString).mkString("\n"))

        case e: Throwable => {
          output.print("An unknown error occurred...");
          e.printStackTrace()

          // The parser pops the input stream back onto the queue, so
          // the next call to Statement() will throw the same exact 
          // Exception.  To prevent this from happening, reset the parser:
          parser = new MimirJSqlParser(source);
        }
      }
    } while(!done)
  }

  def handleSelect(sel: Select): Unit = 
  {
    val raw = db.sql.convert(sel)
    handleQuery(raw)
  }

  def handleQuery(raw:Operator) = 
  {
    Timer.monitor("QUERY", output.print(_)) {
      db.query(raw) { output.print(_) }
    }
  }

  def handleDirectQuery(direct: DirectQuery) =
  {
    /*direct.getStatement match {
      case sel: Select => {
        val iter = new JDBCResultIterator(
          SqlUtils.getSchema(sel.getSelectBody, db).map { (_, TString()) },
          sel.getSelectBody,
          db.backend,
          TString()
        )
        output.print(iter)
        iter.close()
      }
      case update => {
        db.backend.update(update.toString);
      }
    }*/
  }

  def handleExplain(explain: Explain): Unit = 
  {
    val raw = db.sql.convert(explain.getSelectBody())
    output.print("------ Raw Query ------")
    output.print(raw.toString)
    db.typechecker.schemaOf(raw)        // <- discard results, just make sure it typechecks
    val optimized = db.compiler.optimize(raw)
    output.print("--- Optimized Query ---")
    output.print(optimized.toString)
    db.typechecker.schemaOf(optimized)  // <- discard results, just make sure it typechecks
    output.print("--- SQL ---")
    try {
      output.print(db.ra.convert(optimized).toString)
    } catch {
      case e:Throwable =>
        output.print("Unavailable: "+e.getMessage())
    }
  }

  def handleAnalyze(analyze: Analyze)
  {
    val rowId = analyze.getRowId()
    val column = analyze.getColumn()
    val assign = analyze.getAssign()
    val query = db.sql.convert(analyze.getSelectBody())

    if(rowId == null){
      output.print("==== Explain Table ====")
      logger.debug("Starting to Explain Table")
      val reasonSets = db.explainer.explainEverything(query)
      logger.debug("Done Explaining Table")
      for(reasonSet <- reasonSets){
        logger.debug(s"Expanding $reasonSet")
        // Workaround for a bug: SQLite crashes if a UDA is run on an empty input
        if(!reasonSet.isEmpty(db)){
          logger.debug(s"Not Empty: \n${reasonSet.argLookup}")
          val count = reasonSet.size(db);
          logger.debug(s"Size = $count")
          val reasons = reasonSet.take(db, 5);
          logger.debug(s"Got ${reasons.size} reasons")
          printReasons(reasons);
          if(count > reasons.size){
            output.print(s"... and ${count - reasons.size} more like the last")
          }
        }
      }
      if(assign == true) {
        CTPrioritizer.prioritize(reasonSets.flatMap(x=>x.all(db)))
      }
    } else {
      val token = RowIdPrimitive(db.sql.convert(rowId).asString)
      if(column == null){ 
        output.print("==== Explain Row ====")
        val explanation = 
          db.explainer.explainRow(query, token)
        printReasons(explanation.reasons)
        output.print("--------")
        output.print("Row Probability: "+explanation.probability)
        if(assign == true) {
          CTPrioritizer.prioritize(explanation.reasons)
        }
      } else {
      output.print("==== Explain Cell ====")
        val explanation = 
          db.explainer.explainCell(query, token, column) 
        printReasons(explanation.reasons)
        output.print("--------")
        output.print("Examples: "+explanation.examples.map(_.toString).mkString(", "))
        if(assign == true) {
          CTPrioritizer.prioritize(explanation.reasons)
        }
      }
    }
  }

  def printReasons(reasons: Iterable[Reason])
  {
    for(reason <- reasons.toSeq.sortBy( r => if(r.confirmed){ 1 } else { 0 } )){
      val argString = 
        if(!reason.args.isEmpty){
          " (" + reason.args.mkString(",") + ")"
        } else { "" }
      output.print(reason.reason)
      if(!reason.confirmed){
        output.print(s"   ... repair with `FEEDBACK ${reason.model.name} ${reason.idx}$argString IS ${ reason.repair.exampleString }`");
        output.print(s"   ... confirm with `FEEDBACK ${reason.model.name} ${reason.idx}$argString IS ${ reason.guess }`");
      } else {
        output.print(s"   ... ammend with `FEEDBACK ${reason.model.name} ${reason.idx}$argString IS ${ reason.repair.exampleString }`");
      }
      output.print("")
    }
  }

  def handlePragma(pragma: Pragma): Unit = 
  {
    db.sql.convert(pragma.getExpression, (x:String) => x) match {

      case Function("SHOW", Seq(Var("TABLES"))) => 
        for(table <- db.getAllTables()){ output.print(table.toUpperCase); }
      case Function("SHOW", Seq(Var(name))) => 
        db.tableSchema(name) match {
          case None => 
            output.print(s"'$name' is not a table")
          case Some(schema) => 
            output.print("CREATE TABLE "+name+" (\n"+
              schema.map { col => "  "+col._1+" "+col._2 }.mkString(",\n")
            +"\n);")
        }
      case Function("SHOW", _) => 
        output.print("Syntax: SHOW(TABLES) | SHOW(tableName)")

      case Function("LOG", Seq(StringPrimitive(loggerName))) => 
        setLogLevel(loggerName)

      case Function("LOG", Seq(StringPrimitive(loggerName), Var(level))) => 
        setLogLevel(loggerName, level)
      case Function("LOG", _) =>
        output.print("Syntax: LOG('logger') | LOG('logger', TRACE|DEBUG|INFO|WARN|ERROR)");

      case Function("TEST_PYTHON", args) =>
        val p = PythonProcess(s"test ${args.map { _.toString }.mkString(" ")}")
        output.print(s"Python Exited: ${p.exitValue()}")

    }

  }

  def setLogLevel(loggerName: String, levelString: String = "DEBUG")
  {
    val newLevel = internalSetLogLevel(LoggerFactory.getLogger(loggerName), levelString);
    output.print(s"$loggerName <- $newLevel")
  }

  private def internalSetLogLevel(genericLogger: Object, levelString: String): String =
  {
    genericLogger match {
      case logger: ch.qos.logback.classic.Logger => 
        // base logger instance.  Set the logger
        val level = levelString.toUpperCase match {
          case "TRACE" => ch.qos.logback.classic.Level.TRACE
          case "DEBUG" => ch.qos.logback.classic.Level.DEBUG
          case "INFO"  => ch.qos.logback.classic.Level.INFO
          case "WARN"  => ch.qos.logback.classic.Level.WARN
          case "ERROR" => ch.qos.logback.classic.Level.ERROR
          case _ => throw new SQLException(s"Invalid log level: $levelString");
        }
        logger.setLevel(level)
        return level.toString

      case logger: com.typesafe.scalalogging.slf4j.Logger =>
        // SLF4J wraps existing loggers.  Recur to get the real logger 
        return internalSetLogLevel(logger.underlying, levelString)

      case _ => throw new SQLException(s"Don't know how to handle logger ${logger.getClass().toString}")
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