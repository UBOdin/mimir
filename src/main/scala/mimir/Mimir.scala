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
import mimir.metadata._
import mimir.util.{Timer,ExperimentalOptions,LineReaderInputSource,PythonProcess,SqlUtils}
import mimir.data.staging.{ RawFileProvider, LocalFSRawFileProvider }
import mimir.algebra._
import mimir.statistics.{DetectSeries,DatasetShape}
import mimir.plot.Plot
import mimir.exec.{OutputFormat,DefaultOutputFormat,PrettyOutputFormat}
import mimir.exec.spark.MimirSpark

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

  var db: Database = null;
  lazy val terminal: Terminal = TerminalBuilder.terminal()
  var output: OutputFormat = DefaultOutputFormat

  def main(args: Array[String]) = 
  {
    val conf = new MimirConfig(args);

    // Prepare experiments
    ExperimentalOptions.enable(conf.experimental())

   
    val staging = new LocalFSRawFileProvider(new java.io.File(conf.dataDirectory()))

    // Set up the database connection(s)
    MimirSpark.init(conf)
    if(!conf.quiet()){
      output.print("Connecting to metadata provider [" + conf.metadataBackend() + "://" + conf.dbname() + "]...")
    }
    val metadata = new JDBCMetadataBackend(conf.metadataBackend(), conf.dbname())

    db = new Database(metadata, staging)
    logger.debug("Opening Database")
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
          logger.debug(s"Processing file '$file'")
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
                  noninteractiveEventLoop(new FileReader(file))
                  finishByReadingFromConsole = false
                }
              case "csv" => {
                  output.print("Loading "+file+"...")
                  db.loader.loadTable(file)
                }
              case _ => {
                throw new RuntimeException("Error: Unknown file format '"+extension+"' of "+file)
              }
            }
          }
        }
    }
    logger.debug("Checking if console needed")
    if(finishByReadingFromConsole){
      logger.debug("Starting interactive mode")
      interactiveEventLoop()
    }

    db.close()
    if(!conf.quiet()) { output.print("\n\nDone.  Exiting."); }
  }

  def interactiveEventLoop(): Unit =
  {
    eventLoop(
      new LineReaderParser(terminal), 
      (parser: LineReaderParser) => {parser.flush(); parser}
    )
  }

  def noninteractiveEventLoop(source: Reader): Unit =
  {
    eventLoop(
      MimirCommand(source), 
      (_:Any)=>MimirCommand(source)
    )
  }

  def eventLoop[I <: Iterator[Parsed[MimirCommand]]](initialParser: I, reset: (I => I)): Unit =
  {
    var parser = initialParser

    logger.debug("Event Loop")
    while(parser.hasNext){
      logger.debug("next command!")
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
            output.print(s"Parse Error: ${f.longMsg}")
        }

      } catch {
        case e: EOFException => 
          return

        case e: FileNotFoundException =>
          output.print(e.getMessage)

        case e: SQLException =>
          output.print("Error: "+e.getMessage)
          logger.debug(e.getMessage + "\n" + e.getStackTrace.map(_.toString).mkString("\n"))

        case e: RAException =>
          output.print("Error: "+e.getMessage)
          logger.debug(e.getMessage + "\n" + e.getStackTrace.map(_.toString).mkString("\n"))

        case e: org.apache.spark.sql.AnalysisException =>
          output.print("Error: "+e.getMessage)
          logger.debug(e.getMessage + "\n" + e.getStackTrace.map(_.toString).mkString("\n"))

        case e: Throwable => {
          output.print("An unknown error occurred...");
          e.printStackTrace()

          // The parser pops the input stream back onto the queue, so
          // the next call to Statement() will throw the same exact 
          // Exception.  To prevent this from happening, reset the parser:
          parser = reset(parser)
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
        db.raToSpark.mimirOpToSparkOp(optimized).toString
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

    logger.debug("Starting to Analyze Table")
    val reasonSets = 
      analyze.rowid match {
        case None => {
          output.print("==== Analyze Table ====")
          db.uncertainty.explainEverything(query)
        }
        case Some(rowid) => {
          val token = RowIdPrimitive(db.sqlToRA(rowid).asString)
          analyze.column match {
            case None => { 
              output.print("==== Analyze Row ====")
              db.uncertainty.explainSubset(
                oper = query, 
                wantCol = query.columnNames.toSet, 
                wantRow = true, 
                wantSort = false,
                wantSchema = true
              )
            }
            case Some(column) => {
              output.print("==== Analyze Cell ====")
              db.uncertainty.explainSubset(
                oper = query, 
                wantCol = Set(OperatorUtils.columnLookupFunction(query)(column)), 
                wantRow = true, 
                wantSort = false,
                wantSchema = true
              )
            }
          }
        }
      }
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
    logger.debug("Done Analyzing Table")
  }

  def printReasons(reasons: Iterable[Reason])
  {
    for(reason <- reasons.toSeq.sortBy( r => if(r.acknowledged){ 1 } else { 0 } )){
      val argString = 
        if(!reason.key.isEmpty){
          " (" + reason.key.mkString(",") + ")"
        } else { "" }
      output.print(reason.message)
      // reason match {
      //   case _:SimpleCaveatReason => 
      //     if(!reason.acknowledged) { 
      //       output.print(s"    ... acknowledge with `FEEDBACK ${reason.model.name} ${reason.idx}$argString IS ${reason.repair.exampleString}`")
      //     }
      //   case _ => 
      //     if(!reason.confirmed){
      //       output.print(s"   ... repair with `FEEDBACK ${reason.model.name} ${reason.idx}$argString IS ${ reason.repair.exampleString }`");
      //       output.print(s"   ... confirm with `FEEDBACK ${reason.model.name} ${reason.idx}$argString IS ${ reason.guess }`");
      //     } else {
      //       output.print(s"   ... ammend with `FEEDBACK ${reason.model.name} ${reason.idx}$argString IS ${ reason.repair.exampleString }`");
      //     }
      // }
      // output.print("")
    }
  }

  val slashCommands: Map[String, SlashCommandDefinition] = Map(
    MakeSlashCommand(
      "help",
      "",
      "Show this help message",
      { case _ => {
        val cmdWidth = slashCommands.values.map { _.name.length }.max+1
        val argWidth = slashCommands.values.map { _.args.length }.max
        output.print("")
        for(cmd <- slashCommands.values){
          output.print(
            String.format(s" %${cmdWidth}s %-${argWidth}s   %s",
              "/"+cmd.name, cmd.args, cmd.description
            )
          )
        }
        output.print("")
      }}
    ),
    MakeSlashCommand(
      "tables",
      "",
      "List all available tables",
      { case _ => handleQuery(db.catalog.tableView) }
    ),
    MakeSlashCommand(
      "show",
      "table_name",
      "Show the schema for a specified table",
      { case Seq(name) => 
          db.catalog.tableSchema(Name(name)) match {
            case None => 
              output.print(s"'$name' is not a table")
            case Some( schema ) => 
              output.print("CREATE TABLE "+name+" (\n"+
                schema.map { col => "  "+col._1+" "+col._2 }.mkString(",\n")
              +"\n);")
          }
      }
    ),
    MakeSlashCommand(
      "listFunctions",
      "",
      "List all available functions",
      { case Seq() =>
          for((fn, _) <- db.functions.functionPrototypes.toSeq.sortBy { _._1.id }){
            output.print(fn.id)
          }
      }
    ),
    MakeSlashCommand(
      "log",
      "unit [level]",
      "Set the logging level for the specified unit (e.g., DEBUG)",
      { case Seq(loggerName) => setLogLevel(loggerName)
        case Seq(loggerName, level) => setLogLevel(loggerName, level)
      }
    )
  )

  def handleSlashCommand(cmd: SlashCommand): Unit = 
  {
    cmd.body.split(" +").toSeq match { 
      case Seq() => 
        output.print("Empty command")
      case cmd => 
        slashCommands.get(cmd.head) match {
          case None => s"Unknown command: /${cmd.head} (try /help)"
          case Some(implementation) => implementation(cmd.tail, output)
        }
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
  val metadataBackend = opt[String]("driver", descr = "Which metadata backend to use? ([sqlite])",
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
  val googleSheetsCredentialPath = opt[String]("sheetCred", descr = "Credential file for google sheets",
    default = Some("test/data/api-project-378720062738-5923e0b6125f"))
  def dbname : ScallopOption[String] = { 
    opt[String]("db", descr = "Connect to the database with the specified name",
    default = Some(dataDirectory() + "/mimir.db"))
  }
  def sparkJars : ScallopOption[String] = { 
    opt[String]("sparkJars", descr = "Folder with additional jars for spark to load",
    default = Some(dataDirectory() + "/sparkJars"))
  }
}

class SlashCommandDefinition(
  val name: String,
  val args: String,
  val description: String,
  val implementation: PartialFunction[Seq[String], Unit]
){
  def apply(args: Seq[String], output: OutputFormat): Unit = 
    if(implementation.isDefinedAt(args)){ implementation(args) }
    else { output.print(s"usage: $usage")}
  def usage: String = s"/$name $args"
}
object MakeSlashCommand
{
  def apply(
    name: String,
    args: String,
    description: String,
    implementation: PartialFunction[Seq[String], Unit]
  ) = { (name, new SlashCommandDefinition(name, args, description, implementation)) }
}