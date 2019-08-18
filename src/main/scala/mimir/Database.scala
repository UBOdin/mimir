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
import mimir.ctables.{
  AnalyzeUncertainty, 
  OperatorDeterminism, 
  CellExplanation, 
  RowExplanation, 
  InlineVGTerms, 
  CoarseDependency
}
import mimir.data.LoadedTables
import mimir.data.staging.{ RawFileProvider, LocalFSRawFileProvider }
import mimir.exec.Compiler
import mimir.exec.mode.{CompileMode, BestGuess}
import mimir.exec.result.{ResultIterator,SampleResultIterator,Row}
import mimir.exec.spark.MimirSpark
import mimir.lenses.{LensManager}
import mimir.metadata.MetadataBackend
import mimir.models.Model
import mimir.parser.{
    MimirStatement,
    SQLStatement,
    SlashCommand,
    Analyze,
    AnalyzeFeatures,
    AlterTable,
    Compare,
    CreateAdaptiveSchema,
    CreateLens,
    DrawPlot,
    Feedback,
    Load,
    Reload,
    DropLens,
    DropAdaptiveSchema,
    MimirSQL,
    CreateDependency,
    DropDependency
  }
import mimir.optimizer.operator.OptimizeExpressions
import mimir.sql.{SqlToRA,RAToSql}
import mimir.statistics.FuncDep
import mimir.util.ExperimentalOptions

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
case class Database(
  metadata: mimir.metadata.MetadataBackend, 
  staging: mimir.data.staging.RawFileProvider = new LocalFSRawFileProvider(new java.io.File("."))
)
  extends LazyLogging
{
  //// Data Sources
  val lenses          = new mimir.lenses.LensManager(this)
  val models          = new mimir.models.ModelManager(this)
  val loader          = new mimir.data.LoadedTables(this)
  val views           = new mimir.views.ViewManager(this)
  val tempViews       = new mimir.views.TemporaryViewManager(this)
  val adaptiveSchemas = new mimir.adaptive.AdaptiveSchemaManager(this)
  val catalog         = new mimir.data.SystemCatalog(this)

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
      case SQLStatement(create: CreateTable) => {
        // Test for now, create a blank empty table
        catalog.materializedTableProvider().createStoredTableAs(
          HardTable(
            create.columns.map { col => (
              ID.lower(col.name), 
              Type.fromString(col.t.lower)
            )},
            Seq()
          ),
          ID.lower(create.name),
          this
        )
      }
      case SQLStatement(create: CreateTableAs) => {
        catalog.materializedTableProvider().createStoredTableAs(
          sqlToRA(create.query),
          ID.lower(create.name),
          this
        )
      }

      /********** CREATE LENS STATEMENTS **********/
      case lens: CreateLens => {
        lenses.create(
          ID.upper(lens.lensType),
          ID.upper(lens.name),
          sqlToRA(lens.body),
          lens.args.map { sqlToRA(_, SqlToRA.literalBindings(_)) }
        )
      }

      /********** CREATE VIEW STATEMENTS **********/
      case SQLStatement(view: CreateView) => {
        val baseQuery = sqlToRA(view.query)
        val optQuery = compiler.optimize(baseQuery)
        val viewID = ID.upper(view.name)

        if(view.temporary) {
          tempViews.put(viewID, optQuery)
        } else {
          views.create(viewID, optQuery)
          if(view.materialized) { views.materialize(viewID) }
        }
      }

      /********** ALTER VIEW STATEMENTS **********/
      case SQLStatement(AlterView(name, op)) => {
        val viewID = ID.upper(name)

        op match {
          case Materialize(true)  => views.materialize(viewID)
          case Materialize(false) => views.dematerialize(viewID)
        }
      }

      /********** ALTER TABLE STATEMENTS **********/
      case AlterTable(targetSchema, target, op) => {
        val (realTargetSchema, realTarget, _) = 
            catalog.resolveTable(targetSchema, target)
                   .getOrElse { throw new SQLException(s"Unknown table ${(targetSchema.toSeq :+ target).mkString(".")}")}

        val targetTable = (realTargetSchema, realTarget)

        op match {
          case CreateDependency(sourceSchema, source) => {
            val (realSourceSchema, realSource, _) = 
              catalog.resolveTable(sourceSchema, source)
                     .getOrElse { throw new SQLException(s"Unknown table ${(sourceSchema.toSeq :+ source).mkString(".")}") }
            catalog.createDependency(targetTable, CoarseDependency(realSourceSchema, realSource))
          }
          case DropDependency(sourceSchema, source) => {
            val (realSourceSchema, realSource, _) = 
              catalog.resolveTable(sourceSchema, source)
                     .getOrElse { throw new SQLException(s"Unknown table ${(sourceSchema.toSeq :+ source).mkString(".")}") }
            catalog.dropDependency(targetTable, CoarseDependency(realSourceSchema, realSource))
          }
        }
      }

      /********** CREATE ADAPTIVE SCHEMA **********/
      case create: CreateAdaptiveSchema => {
        adaptiveSchemas.create(
          ID.upper(create.name),
          ID.upper(create.schemaType),
          sqlToRA(create.body),
          create.args.map( sqlToRA(_, SqlToRA.literalBindings(_)) ),
          create.humanReadableName
        )
      }

      /********** LOAD STATEMENTS **********/
      case load: Load => {
        // Assign a default table name if needed
        val sourceFile = load.file
        val format = ID.lower(load.formatOrElse("csv"))
        val targetTable = load.table.map { ID.upper(_) }
        val sparkOptions = load.args
                              .toMap
                              .mapValues { sqlToRA(_) }
                              .mapValues { _.asString }
        val stageSourceURL = load.withStaging

        if(load.linkOnly){
          loader.linkTable(
            sourceFile = sourceFile,
            format = format,
            targetTable = targetTable.getOrElse(loader.fileToTableName(sourceFile)),
            sparkOptions = sparkOptions,
            stageSourceURL = stageSourceURL
          )
        } else {
          loader.loadTable(
            sourceFile = sourceFile,
            format = format,
            targetTable = targetTable,
            sparkOptions = sparkOptions,
            stageSourceURL = stageSourceURL
          )
        }
      }

      case Reload(table) => {
        loader.reloadTable(
          loader.resolveTableByName(table)
                .getOrElse { throw new SQLException(s"No such table $table")}
        )
      }

      /********** DROP STATEMENTS **********/

        // Lenses show up as views, and the lens manager does a 
        // little extra cleanup that's harmless if applied to a
        // view.  Do the same thing for both
      case SQLStatement(DropView(name, ifExists)) => 
        views.resolveTableByName(name) match {
          case None if ifExists => {}
          case None => throw new SQLException(s"No such view $name")
          case Some(id) => lenses.drop(id)
        }
      case DropLens(name, ifExists)               => 
        views.resolveTableByName(name) match {
          case None if ifExists => {}
          case None => throw new SQLException(s"No such view $name")
          case Some(id) => lenses.drop(id)
        }
      case DropAdaptiveSchema(name, ifExists)     => 
        adaptiveSchemas.dropByName(name, ifExists)
      case SQLStatement(drop:DropTable) => 
        loader.resolveTableByName(drop.name) match {
          case Some(dropTableID) => loader.drop(dropTableID)
          case None => 
            if(!drop.ifExists){
              throw new SQLException(s"No such loaded table ${drop.name}.")
            }
        }

      /********** Update Metadata **********/

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
  def open(): Unit = {
    MimirSpark.linkDBToSpark(this)
    metadata.open()
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


  def table(caseInsensitiveName: String): Operator = 
    table(Name(caseInsensitiveName))
  def table(name: Name): Operator = 
    catalog.tableOperator(name)
  def table(id: ID): Operator = 
    catalog.tableOperator(id)
  def table(caseInsensitiveProvider: String, caseInsensitiveName: String): Operator = 
    table(Name(caseInsensitiveProvider), Name(caseInsensitiveName))
  def table(provider: Name, name: Name): Operator = 
    catalog.tableOperator(provider, name)
  def table(provider: ID, id: ID): Operator = 
    catalog.tableOperator(provider, id)

  def tableExists(caseInsensitiveName: String): Boolean = 
    tableExists(Name(caseInsensitiveName))
  def tableExists(name: Name): Boolean = 
    catalog.tableExists(name)
  def tableExists(id: ID): Boolean = 
    catalog.tableExists(id)

  def tableSchema(caseInsensitiveName: String): Option[Seq[(ID, Type)]] = 
    tableSchema(Name(caseInsensitiveName))
  def tableSchema(name: Name): Option[Seq[(ID, Type)]] = 
    catalog.tableSchema(name)
  def tableSchema(id: ID): Option[Seq[(ID, Type)]] = 
    catalog.tableSchema(id)
  def tableSchema(caseInsensitiveProvider: String, caseInsensitiveName: String): Option[Seq[(ID, Type)]] = 
    tableSchema(Name(caseInsensitiveProvider), Name(caseInsensitiveName))
  def tableSchema(provider: Name, name: Name): Option[Seq[(ID, Type)]] = 
    catalog.tableSchema(provider, name)
  def tableSchema(provider: ID, id: ID): Option[Seq[(ID, Type)]] = 
    catalog.tableSchema(provider, id)

}
