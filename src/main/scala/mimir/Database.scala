package mimir

import java.io.File
import java.io.StringReader
import java.sql.SQLException
import java.sql.ResultSet
import java.net.URL

import sparsity.Name
import sparsity.statement._
import sparsity.alter._
import sparsity.expression.Expression

import mimir.algebra._
import mimir.ctables.{AnalyzeUncertainty, OperatorDeterminism, CellExplanation, RowExplanation, InlineVGTerms}
import mimir.models.Model
import mimir.exec.Compiler
import mimir.exec.mode.{CompileMode, BestGuess}
import mimir.exec.result.{ResultIterator,SampleResultIterator,Row}
import mimir.lenses.{LensManager}
import mimir.sql.{SqlToRA,RAToSql}
import mimir.data.LoadedTables
import mimir.metadata.MetadataBackend
import mimir.parser.{
    MimirStatement,
    SQLStatement,
    SlashCommand,
    Analyze,
    AnalyzeFeatures,
    Compare,
    CreateAdaptiveSchema,
    CreateLens,
    DrawPlot,
    Feedback,
    Load,
    DropLens,
    DropAdaptiveSchema
  }
import mimir.optimizer.operator.OptimizeExpressions
import mimir.util.ExperimentalOptions
import mimir.parser.MimirSQL
import mimir.statistics.FuncDep

import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer


 /**
  * The central dispatcher for Mimir.  Most Mimir functionality makes use of the Relational 
  * Algebra and Expression ASTs in mimir.algebra.{Operator,Expression}, but individual components
  * may make use of SQL or other query representations.  The Database class acts as a bridge
  * between these components, and provides a single, central way to access all of Mimir's resources.
  * As a side effect, this allows us to decouple logic for different parts of Mimir into separate
  * classes, linked only by this central Database class.
  *
  * You should never need to access any of the classes below directly.  If you do, add another
  * accessor method to Database instead.
  *
  * === Parsing ===
  * * mimir.sql.SqlToRA (sql)
  *    Responsible for translating JSqlParser AST elements into corresponding AST elements from
  *    mimir.algebra._  
  * * mimir.sql.RAToSql (ra)
  *    Responsible for translating mimir.algebra._ AST elements back to JSqlParser's AST.  This is
  *    typically only required for compatibility with JDBC.
  * * mimir.parser.OperatorParser (operator)
  *    Responsible for directly constructing mimir.algebra.{Operator,Expression} ASTs from string
  *    representations.  Allows these ASTs to be serialized through toString()
  *
  * === Persistence ===
  * * mimir.views.ViewManager (views)
  *    Responsible for creating, serializing, and deserializing virtual Mimir-level views.
  * * mimir.views.ModelManager (models)
  *    Responsible for creating, serializing, and deserializing models.
  * * mimir.lenses.LensManager (lenses)
  *    Responsible for creating and managing lenses
  * * mimir.adaptive.AdaptiveSchemaManager (adaptiveSchemas)
  *    Responsible for creating and managing adaptive schemas (multilenses)
  *
  * === Logic ===
  * * mimir.sql.Backend (backend)
  *    Pluggable wrapper for database backends over which Mimir will actually run.  Basically,
  *    a simplified form of JDBC.  See mimir.sql._ for examples.
  * * mimir.lenses.LensManager (lenses)
  *    Responsible for creating, serializing, and deserializing lenses and virtual views.
  * * mimir.exec.Compiler (compiler)
  *    Responsible for query execution.  Acts as a wrapper around the logic in mimir.ctables._, 
  *    mimir.lenses._, and mimir.exec._ that prepares non-deterministic queries to be evaluated
  *    on the backend database.  
  * * mimir.statistics.SystemCatalog (catalog)
  *    Responsible for managing the system catalog tables/views
  * * mimir.explainer.CTExplainer (explainer)
  *    Responsible for creating explanation objects.
  */
case class Database(metadata: MetadataBackend)
  extends LazyLogging
{
  //// Data Sources
  val lenses          = new mimir.lenses.LensManager(this)
  val models          = new mimir.models.ModelManager(this)
  val loader          = new mimir.data.LoadedTables(this)
  val views           = new mimir.views.ViewManager(this)
  val transientViews  = new mimir.views.TransientViews(this)
  val adaptiveSchemas = new mimir.adaptive.AdaptiveSchemaManager(this)
  val catalog         = new mimir.metadata.SystemCatalog(this)

  //// Parsing & Translation
  val sqlToRA         = new mimir.sql.SqlToRA(this)
  val raToSQL         = new mimir.sql.RAToSql(this)
  val raToSpark       = new mimir.exec.spark.RAToSpark(this)

  // User-defined functionality
  val functions       = new mimir.algebra.function.FunctionRegistry()
  val aggregates      = new mimir.algebra.function.AggregateRegistry()

  //// Logic
  val compiler        = new mimir.exec.Compiler(this)
  val uncertainty     = new mimir.ctables.AnalyzeUncertainty(this)
  val typechecker     = new mimir.algebra.Typechecker(
                                  functions = Some(functions), 
                                  aggregates = Some(aggregates),
                                  models = Some(models)
                                )
  val interpreter     = new mimir.algebra.Eval(
                                  functions = Some(functions)
                                )  

  /**
   * Optimize and evaluate the specified query.  Applies all Mimir-specific optimizations
   * and rewrites the query to properly account for Virtual Tables.
   */
  final def query[T, R <:ResultIterator](oper: Operator, mode: CompileMode[R])(handler: R => T): T =
  {
    val iterator = mode(this, views.rebuildAdaptiveViews(oper), compiler.sparkBackendRootIterator)
    try {
      val ret = handler(iterator)

      // A bit of a hack, but necessary for safety...
      // The iterator we pass to the handler is only valid within this block.
      // It is incredibly easy to accidentally have the handler return the
      // iterator as-is.  For example:
      // > 
      // > query(...) { _.map { ... } }
      // >
      // In this above example, the return value of the block becomes invalid,
      // since a map of an iterator doesn't drain the iterator, but simply applies
      // a continuation to it.  
      // 
      if(ret.isInstanceOf[Iterator[_]]){
        logger.warn("Returning a sequence from Database.query may lead to the Scala compiler's optimizations closing the ResultIterator before it's fully drained")
      }
      return ret
    } finally {
      iterator.close()
    }
  }

  /**
   * Optimize and evaluate the specified query.  Applies all Mimir-specific optimizations
   * and rewrites the query to properly account for Virtual Tables.
   */
  final def query[T](oper: Operator)(handler: ResultIterator => T): T =
    query(oper, BestGuess)(handler)


  /**
   * Evaluate a statement that does not produce results.
   *
   * Generally these are routed directly to the back-end, but there
   * are a few operations that Mimir needs to handle directly.
   */
  def update(stmt: MimirStatement)
  {
    stmt match {
      /********** QUERY STATEMENTS **********/
      case SQLStatement(_:sparsity.statement.Select) 
                          => throw new SQLException("Can't evaluate SELECT as an update")
      case SQLStatement(_:sparsity.statement.Explain)
                          => throw new SQLException("Can't evaluate EXPLAIN as an update")
      case _:Analyze      => throw new SQLException("Can't evaluate ANALYZE as an update")
      case _:AnalyzeFeatures => throw new SQLException("Can't evaluate ANALYZE as an update")
      case _:Compare      => throw new SQLException("Can't evaluate COMPARE as an update")
      case _:DrawPlot     => throw new SQLException("Can't evaluate DRAW PLOT as an update")

      /********** FEEDBACK STATEMENTS **********/
      case feedback: Feedback => {
        val model = models.get(ID.upper(feedback.model))
        val args =
          feedback.args
            .map { case p:sparsity.expression.PrimitiveValue => sqlToRA(p)
                   case v => throw new SQLException(s"Invalid Feedback Argument '$v'") }
            .zip( model.argTypes(feedback.index.toInt) )
            .map { case (v, t) => Cast(t, v) }
        val v = sqlToRA(feedback.value)

        model.feedback(feedback.index.toInt, args, v)
        models.persist(model)
      }

      /********** CREATE TABLE STATEMENTS **********/
      case SQLStatement(_:sparsity.statement.CreateTable)
                          => throw new SQLException("CREATE TABLE not presently supported")

      /********** CREATE LENS STATEMENTS **********/
      case lens: CreateLens => {
        lenses.create(
          ID.upper(lens.lensType),
          ID.upper(lens.name),
          sqlToRA(lens.body),
          lens.args.map { sqlToRA(_, sqlToRA.literalBindings(_)) }
        )
      }

      /********** CREATE VIEW STATEMENTS **********/
      case SQLStatement(view: CreateView) => {
        val baseQuery = sqlToRA(view.query)
        val optQuery = compiler.optimize(baseQuery)
        val viewID = ID.upper(view.name)

        views.create(viewID, optQuery)
        if(view.materialized) { views.materialize(viewID) }
      }

      /********** ALTER VIEW STATEMENTS **********/
      case SQLStatement(AlterView(name, op)) => {
        val viewID = ID.upper(name)

        op match {
          case Materialize(true)  => views.materialize(viewID)
          case Materialize(false) => views.dematerialize(viewID)
        }
      }

      /********** CREATE ADAPTIVE SCHEMA **********/
      case create: CreateAdaptiveSchema => {
        adaptiveSchemas.create(
          ID.upper(create.name),
          ID.upper(create.schemaType),
          sqlToRA(create.body),
          create.args.map( sqlToRA(_, sqlToRA.literalBindings(_)) ),
          create.humanReadableName
        )
      }

      /********** LOAD STATEMENTS **********/
      case load: Load => {
        // Assign a default table name if needed
        loadTable(
          load.file, 
          targetTable = load.table.map { ID.upper(_) },
          // force = (load.table != None),
          format = load.format
                       .getOrElse { sparsity.Name("csv") }
                       .lower,
          loadOptions = load.args
                            .toMap
                            .mapValues { sqlToRA(_) }
                            .mapValues { _.asString }
        )
      }

      /********** DROP STATEMENTS **********/

      case SQLStatement(DropView(name, ifExists)) => views.drop(ID.upper(name), ifExists)
      case DropLens(name, ifExists)               => lenses.drop(ID.upper(name), ifExists)
      case DropAdaptiveSchema(name, ifExists)     => adaptiveSchemas.drop(ID.upper(name), ifExists)

      /********** Update Metadata **********/
      case SQLStatement(drop:DropTable) => 
        throw new SQLException("DROP not supported")

      case SQLStatement(update: Update) => 
        throw new SQLException("UPDATE not supported")

      case SQLStatement(update: Insert) => 
        throw new SQLException("INSERT not supported")

      case SQLStatement(update: Delete) => 
        throw new SQLException("DELETE not supported")      
    }
  }
  
  /**
   * Prepare a database for use with Mimir.
   */
  def open(skipBackend: Boolean = false): Unit = {
    if(!skipBackend){
      metadata.open()
    }
    catalog.init()
    loader.init()
    models.init()
    views.init()
    lenses.init()
    adaptiveSchemas.init()
  }

  def close(): Unit = {
    metadata.close()
  }

  /**
   * Load a CSV file into the database
   *
   * The CSV file can either have a header or not
   *  - If the file has a header, the first line will be skipped
   *    during the insert process
   *
   *  - If the file does not have a header, the table must exist
   *    in the database, created through a CREATE TABLE statement
   *    Otherwise, a SQLException will be thrown
   *
   * Right now, the detection logic for whether a CSV file has a
   * header or not is unimplemented. So its assumed every CSV file
   * supplies an appropriate header.
   */
  def fileToTableName(file: String): ID =
    ID(new File(file).getName.replaceAll("\\..*", ""))
  

  def loadTable(
    sourceFile: String, 
    targetTable: Option[ID] = None,
    // targetSchema: Option[Seq[(ID, Type)]] = None,
    inferTypes: Option[Boolean] = None,
    detectHeaders: Option[Boolean] = None,
    format: String = LoadedTables.CSV,
    loadOptions: Map[String, String] = Map(),
    humanReadableName: Option[String] = None
  ){
    // Pick a sane table name if necessary
    val realTargetTable = targetTable.getOrElse(fileToTableName(sourceFile))

    // If the backend is configured to support it, specialize data loading to support data warnings
    val datasourceErrors = loadOptions.getOrElse("datasourceErrors", "false").equals("true")
    val realFormat = 
      format match {
        case LoadedTables.CSV if datasourceErrors => 
          LoadedTables.CSV_WITH_ERRORCHECKING
        case _ => format
      }

    val targetRaw = realTargetTable.withSuffix("_RAW")
    if(catalog.tableExists(targetRaw)){
      throw new SQLException(s"Target table $realTargetTable already exists")
    }
    loader.linkTable(
      url = sourceFile,
      format = realFormat,
      tableName = targetRaw,
      sparkOptions = loadOptions
    )
    var oper = loader.tableOperator(targetRaw, targetRaw)
    //detect headers 
    if(datasourceErrors) {
      val dseSchemaName = realTargetTable.withSuffix("_DSE")
      adaptiveSchemas.create(dseSchemaName, ID("DATASOURCE_ERRORS"), oper, Seq(), humanReadableName.getOrElse(realTargetTable.id))
      oper = adaptiveSchemas.viewFor(dseSchemaName, ID("DATA")).get
    }
    if(detectHeaders.getOrElse(true)) {
      val dhSchemaName = realTargetTable.withSuffix("_DH")
      adaptiveSchemas.create(dhSchemaName, ID("DETECT_HEADER"), oper, Seq(), humanReadableName.getOrElse(realTargetTable.id))
      oper = adaptiveSchemas.viewFor(dhSchemaName, ID("DATA")).get
    }
    //type inference
    if(inferTypes.getOrElse(true)){
      val tiSchemaName = realTargetTable.withSuffix("_TI")
      adaptiveSchemas.create(tiSchemaName, ID("TYPE_INFERENCE"), oper, Seq(FloatPrimitive(.5)), humanReadableName.getOrElse(realTargetTable.id)) 
      oper = adaptiveSchemas.viewFor(tiSchemaName, ID("DATA")).get
    }
    //finally create a view for the data
    views.create(realTargetTable, oper)
  }

  // /**
  //   * Materialize a view into the database
  //   */
  // def selectInto(targetTable: ID, sourceQuery: Operator){
  //   backend.createTable(targetTable, sourceQuery)
  // }
}
