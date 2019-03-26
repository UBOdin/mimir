package mimir;

import java.io._
import java.sql.SQLException
import java.net.URL

import org.jline.terminal.{Terminal,TerminalBuilder}
import org.slf4j.{LoggerFactory}
import org.rogach.scallop._
import com.typesafe.scalalogging.slf4j.LazyLogging
import scala.collection.JavaConverters._

import sparsity._
import fastparse.Parsed

import mimir.ctables._
import mimir.parser._
import mimir.sql._
import mimir.backend._
import mimir.util.{Timer,ExperimentalOptions,LineReaderInputSource,PythonProcess,SqlUtils}
import mimir.algebra._
import mimir.algebra.spark.OperatorTranslation
import mimir.statistics.{DetectSeries,DatasetShape}
import mimir.plot.Plot
import mimir.exec.{OutputFormat,DefaultOutputFormat,PrettyOutputFormat}
import mimir.exec.result.JDBCResultIterator

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
    val database = conf.dbname().split("[\\\\/]").last.replaceAll("\\..*", "")
    val sback = new SparkBackend(database)
    db = new Database(sback, new JDBCMetadataBackend(conf.backend(), conf.dbname()))
    if(!conf.quiet()){
      output.print("Connecting to " + conf.backend() + "://" + conf.dbname() + "...")
    }
    db.metadataBackend.open()
    db.backend.open()
    OperatorTranslation.db = db
    sback.registerSparkFunctions(
      db.functions.functionPrototypes.map { _._1 }.toSeq,
      db.functions
    )
    sback.registerSparkAggregates(
      db.aggregates.prototypes.map { _._1 }.toSeq,
      db.aggregates
    )
      
    db.initializeDBForMimir();

    // Check for one-off commands
    if(conf.loadTable.get != None){
      db.loadTable(conf.loadTable());
    } else {

      conf.precache.foreach( (opt) => opt.split(",").foreach( (table) => { 
        output.print(s"Precaching... $table")
        db.models.prefetchForOwner(ID.upper(table))
      }))

      if(!ExperimentalOptions.isEnabled("NO-INLINE-VG")){
        db.metadataBackend.asInstanceOf[InlinableBackend].enableInlining(db)
      }
      if(!ExperimentalOptions.isEnabled("SIMPLE-TERM")){
        output = new PrettyOutputFormat(terminal)
      }
      if(!conf.quiet()){
        output.print("   ... ready")
      }

      var finishByReadingFromConsole = true

      conf.files.get match {
        case None => {}
        case Some(files) => 
          for(file <- files){
            if(file == "-"){
              interactiveEventLoop()
              finishByReadingFromConsole = false
            } else {
              val extensionRegexp = "([^.]+)$".r
              val extension:String = extensionRegexp.findFirstIn(file) match {
                case Some(e) => e
                case None => {
                  throw new RuntimeException("Error: Unable to determine file format of "+file)
                }
              }

              extension.toLowerCase match {
                case "sql" => {
                    eventLoop(new FileReader(file), { () =>  })
                    finishByReadingFromConsole = false
                  }
                case "csv" => {
                    output.print("Loading "+file+"...")
                    db.loadTable(file)
                  }
                case _ => {
                  throw new RuntimeException("Error: Unknown file format '"+extension+"' of "+file)
                }
              }
            }
          }
      }
      if(finishByReadingFromConsole){
        interactiveEventLoop()
      }
    }

    db.backend.close()
    if(!conf.quiet()) { output.print("\n\nDone.  Exiting."); }
  }

  def interactiveEventLoop(): Unit =
  {
    val (source, prompt) = 
      if(!ExperimentalOptions.isEnabled("SIMPLE-TERM")){
        (
          new LineReaderInputSource(terminal),
          { () =>  }
        )
      } else {
        (
          new InputStreamReader(System.in),
          () => { System.out.print("\nmimir> "); System.out.flush(); }
        )
      }
    eventLoop(source, prompt)
  }

  def eventLoop(source: Reader, prompt: (() => Unit)): Unit =
  {
    var parser = MimirCommand(source);
    var done = false;

    prompt()
    while(parser.hasNext){
      try {
        parser.next() match {
          case Parsed.Success(cmd:SlashCommand, _) => 
            handleSlashCommand(cmd)

          case Parsed.Success(SQLCommand(stmt), _) =>
            stmt match {
              case SQLStatement(sel:  sparsity.statement.Select) => 
                handleSelect(sel)
              case SQLStatement(expl: sparsity.statement.Explain)    => 
                handleExplain(expl)
              case analyze: Analyze => 
                handleAnalyze(analyze)
              case plot: DrawPlot   => 
                Plot.plot(plot, db, output)
              case compare: Compare => 
                handleCompare(compare)
              case _                => 
                db.update(stmt)
            }

          case f: Parsed.Failure => 
            output.print(s"Parse Error: ${f.msg}")
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
          parser = MimirCommand(source);
        }
      }
    }
  }

  def handleSelect(sel: sparsity.statement.Select): Unit = 
  {
    val raw = db.sqlToRA(sel)
    handleQuery(raw)
  }

  def handleQuery(raw:Operator) = 
  {
    Timer.monitor("QUERY", output.print(_)) {
      db.query(raw) { output.print(_) }
    }
  }

  def handleCompare(comparison: Compare): Unit =
  {
    val target = db.sqlToRA(comparison.target)
    val expected = db.sqlToRA(comparison.expected)

    val facets = DatasetShape.detect(db, expected)
    output.print("---- Comparison Dataset Features ----")
    for(facet <- facets){
      output.print(" > "+facet.description);
    }
    output.print("---- Target Differences ----")
    for(facet <- facets){
      for(difference <- facet.test(db, target)){
        output.print(" > "+difference)
      }
    }
  }

  def handleExplain(explain: sparsity.statement.Explain): Unit = 
  {
    val raw = db.sqlToRA(explain.query)
    output.print("------ Raw Query ------")
    output.print(raw.toString)
    db.typechecker.schemaOf(raw)        // <- discard results, just make sure it typechecks
    val optimized = db.compiler.optimize(raw)
    output.print("\n--- Optimized Query ---")
    output.print(optimized.toString)
    db.typechecker.schemaOf(optimized)  // <- discard results, just make sure it typechecks
    output.print("\n-------- SQL --------")
    try {
      output.print(db.raToSQL(optimized).toString)
    } catch {
      case e:Throwable =>
        output.print("Unavailable: "+e.getMessage())
    }
    output.print("\n------- Spark -------")
    try {
      output.print(
        mimir.algebra.spark.OperatorTranslation.mimirOpToSparkOp(optimized).toString
      )
    } catch {
      case e:Throwable =>
        output.print("Unavailable: "+e.getMessage())
    }
  }

  def handleAnalyzeFeatures(analyze: AnalyzeFeatures)
  {
    val query = db.sqlToRA(analyze.target)
    output.print("==== Analyze Features ====")
    for(facet <- DatasetShape.detect(db, query)) {
      output.print(s"  > ${facet.description}")
    }
  }

  def handleAnalyze(analyze: Analyze)
  {
    val column = analyze.column      // The column of the cell to analyze, or null if full row or table scan
    val query = db.sqlToRA(analyze.target)

    analyze.rowid match {
      case None => {
        output.print("==== Analyze Table ====")
        logger.debug("Starting to Analyze Table")
        val reasonSets = db.explainer.explainEverything(query)
        logger.debug("Done Analyzing Table")
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
        if(analyze.withAssignments) {
          CTPrioritizer.prioritize(reasonSets.flatMap(x=>x.all(db)))
        }
      }
      case Some(rowid) => {
        val token = RowIdPrimitive(db.sqlToRA(rowid).asString)
        analyze.column match {
          case None => { 
            output.print("==== Analyze Row ====")
            val explanation = 
              db.explainer.explainRow(query, token)
            printReasons(explanation.reasons)
            output.print("--------")
            output.print("Row Probability: "+explanation.probability)
            if(analyze.withAssignments) {
              CTPrioritizer.prioritize(explanation.reasons)
            }
          }
          case Some(column) => {
            output.print("==== Analyze Cell ====")
            val explanation = 
              db.explainer.explainCell(query, token, ID.upper(column))
            printReasons(explanation.reasons)
            output.print("--------")
            output.print("Examples: "+explanation.examples.map(_.toString).mkString(", "))
            if(analyze.withAssignments) {
              CTPrioritizer.prioritize(explanation.reasons)
            }
          }
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
      reason match {
        case _:DataWarningReason => 
          if(!reason.confirmed) { 
            output.print(s"    ... acknowledge with `FEEDBACK ${reason.model.name} ${reason.idx}$argString IS ${reason.repair.exampleString}`")
          }
        case _ => 
          if(!reason.confirmed){
            output.print(s"   ... repair with `FEEDBACK ${reason.model.name} ${reason.idx}$argString IS ${ reason.repair.exampleString }`");
            output.print(s"   ... confirm with `FEEDBACK ${reason.model.name} ${reason.idx}$argString IS ${ reason.guess }`");
          } else {
            output.print(s"   ... ammend with `FEEDBACK ${reason.model.name} ${reason.idx}$argString IS ${ reason.repair.exampleString }`");
          }
      }
      output.print("")
    }
  }

  def handleSlashCommand(cmd: SlashCommand): Unit = 
  {
    cmd.body.split(" +").toSeq match { 
      case Seq("show", "tables") => 
        for(table <- db.getAllTables()){ output.print(table.toString); }
      case Seq("show", name) => 
        db.tableSchema(ID.upper(name)) match {
          case None => 
            output.print(s"'$name' is not a table")
          case Some(schema) => 
            output.print("CREATE TABLE "+name+" (\n"+
              schema.map { col => "  "+col._1+" "+col._2 }.mkString(",\n")
            +"\n);")
        }
      case Seq("log", loggerName) => 
        setLogLevel(loggerName)
      case Seq("log", loggerName, level) =>
        setLogLevel(loggerName, level)
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
  val backend = opt[String]("driver", descr = "Which backend database to use? ([sqlite],oracle)",
    default = Some("sqlite"))
  val precache = opt[String]("precache", descr = "Precache one or more lenses")
  val rebuildBestGuess = opt[String]("rebuild-bestguess")  
  val quiet  = toggle("quiet", default = Some(false))
  val files = trailArg[List[String]](required = false)
  val experimental = opt[List[String]]("X", default = Some(List[String]()))
  val mimirHost = opt[String]("mimirHost", descr = "The IP or hostname of mimir",
    default = Some("vizier-mimir.local"))
  val sparkHost = opt[String]("sparkHost", descr = "The IP or hostname of the spark master",
    default = Some("spark-master.local"))
  val sparkPort = opt[String]("sparkPort", descr = "The port of the spark master",
    default = Some("7077"))
  val sparkDriverMem = opt[String]("sparkDriverMem", descr = "The memory for spark driver",
    default = Some("8g"))
  val sparkExecutorMem = opt[String]("sparkExecutorMem", descr = "The memory for spark executors",
    default = Some("8g"))
  val hdfsPort = opt[String]("hdfsPort", descr = "The port for hdfs",
    default = Some("8020"))
  val useHDFSHostnames = toggle("useHDFSHostnames", default = Some(Option(System.getenv("HDFS_CONF_dfs_client_use_datanode_hostname")).getOrElse("false").toBoolean),
      descrYes = "use the hostnames for hdfs nodes",
      descrNo = "use ip addresses for hdfs nodes")
  val overwriteStagedFiles = toggle("overwriteStagedFiles", default = Some(false),
      descrYes = "overwrites files sent to staging area (hdfs or s3)",
      descrNo = "do not overwrites files sent to staging area (hdfs or s3)")
  val overwriteJars = toggle("overwriteJars", default = Some(false),
      descrYes = "overwrites jar files sent to hdfs",
      descrNo = "do not overwrites jar files sent to hdfs")
  val numPartitions = opt[Int]("numPartitions", descr = "number of partitions to use",
    default = Some(8))
  val dataStagingType = opt[String]("dataStagingType", descr = "where to stage data for spark: hdfs or s3",
    default = Some("hdfs"))
  val dataDirectory = opt[String]("dataDirectory", descr = "The directory to place data files",
    default = Some("."))
  def dbname : ScallopOption[String] = { 
    opt[String]("db", descr = "Connect to the database with the specified name",
    default = Some(dataDirectory() + "/debug.db"))
  }

}