package mimir

import java.io.File
import java.io.StringReader
import java.sql.SQLException
import java.sql.ResultSet

import mimir.algebra._
import mimir.ctables.{CTExplainer, CTPercolator, CellExplanation, RowExplanation, VGTerm}
import mimir.models.Model
import mimir.exec.Compiler
import mimir.exec.result.{ResultIterator,SampleResultIterator,Row}
import mimir.lenses.{LensManager, BestGuessCache}
import mimir.parser.OperatorParser
import mimir.sql.{SqlToRA,RAToSql,Backend}
import mimir.sql.{
    CreateLens,
    CreateView,
    Explain,
    Feedback,
    Load,
    Pragma,
    Analyze,
    CreateAdaptiveSchema,
    AlterViewMaterialize
  }
import mimir.optimizer.{InlineVGTerms}
import mimir.util.{LoadCSV,ExperimentalOptions}
import mimir.parser.MimirJSqlParser
import mimir.statistics.FuncDep

import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.select.Select
import net.sf.jsqlparser.statement.create.table.CreateTable
import net.sf.jsqlparser.statement.drop.Drop

import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.collection.JavaConversions._
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
case class Database(backend: Backend)
  extends LazyLogging
{
  //// Persistence
  val lenses          = new mimir.lenses.LensManager(this)
  val models          = new mimir.models.ModelManager(this)
  val views           = new mimir.views.ViewManager(this)
  val bestGuessCache  = new mimir.lenses.BestGuessCache(this)
  val querySerializer = new mimir.algebra.Serialization(this)
  val adaptiveSchemas = new mimir.adaptive.AdaptiveSchemaManager(this)

  //// Logic
  val compiler        = new mimir.exec.Compiler(this)
  val explainer       = new mimir.ctables.CTExplainer(this)
  val catalog         = new mimir.statistics.SystemCatalog(this)

  //// Parsing
  val sql             = new mimir.sql.SqlToRA(this)
  val ra              = new mimir.sql.RAToSql(this)
  val operator        = new mimir.parser.OperatorParser(models.get _,
    (x) => 
      this.getTableSchema(x) match {
        case Some(x) => x
        case None => throw new RAException("Table "+x+" does not exist in db!")
      }
  )

  /** 
   * Apply the standard set of Mimir compiler optimizations -- Used mostly for EXPLAIN.
   */
  def optimize(oper: Operator): Operator =
  {
    Compiler.optimize(oper)
  }

  /**
   * Optimize and evaluate the specified query.  Applies all Mimir-specific optimizations
   * and rewrites the query to properly account for Virtual Tables.
   */
  final def query[T](oper: Operator)(handler: ResultIterator => T): T =
  {
    val iterator = compiler.compileForBestGuess(oper)
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
   * Translate, optimize and evaluate the specified query.  Applies all Mimir-specific 
   * optimizations and rewrites the query to properly account for Virtual Tables.
   */
  final def query[T](stmt: net.sf.jsqlparser.statement.select.Select)(handler: ResultIterator => T): T = 
  {
    query(sql.convert(stmt))(handler)
  }

  /**
   * Translate, optimize and evaluate the specified query.  Applies all Mimir-specific 
   * optimizations and rewrites the query to properly account for Virtual Tables.
   */
  final def query[T](stmt: String)(handler: ResultIterator => T): T = 
  {
    query(select(stmt))(handler)
  }

  /**
   * Compute the query result with samples.
   */
  def sampleQuery[T](oper: Operator)(handler: SampleResultIterator => T): T =
  {
    val iterator: SampleResultIterator = compiler.compileForSamples(oper)
    try {
      val ret = handler(iterator)

      // See the comments on query for an explanation on the hackishness here.
      if(ret.isInstanceOf[Iterator[_]]){
        logger.warn("Returning a sequence from Database.sampleQuery may lead to the Scala compiler's optimizations closing the ResultIterator before it's fully drained")
      }
      return ret;
    } finally {
      iterator.close()
    }
  }

  /**
   * Compute the query result with samples.
   */
  def sampleQuery[T](stmt: net.sf.jsqlparser.statement.select.Select)(handler: SampleResultIterator => T): T =
  {
    sampleQuery(sql.convert(stmt))(handler)
  }

  /**
   * Compute the query result with samples.
   */
  def sampleQuery[T](stmt: String)(handler: SampleResultIterator => T): T =
  {
    sampleQuery(select(stmt))(handler)
  }

  /**
   * Make an educated guess about what the query's schema should be
   * XXX: Only required because the TypeInference lens is a little punk birch that needs to
   *      be converted into an adaptive schema (#193).
   */
  def bestGuessSchema(oper: Operator): Seq[(String, Type)] =
  {
    InlineVGTerms(oper).schema
  }

  /**
   * Parse raw SQL data
   */
  def parse(queryString: String): List[Statement] =
  {
    val parser = new MimirJSqlParser(new StringReader(queryString))

    var stmt:Statement = parser.Statement()
    var ret = List[Statement]()

    while( stmt != null ) { ret = stmt :: ret ; stmt = parser.Statement() }

    ret.reverse
  }

  /**
   * Generate an explanation object for a row
   */
  def explainRow(query: Operator, token: RowIdPrimitive): RowExplanation =
    explainer.explainRow(query, token)

  /**
   * Generate an explanation object for a column
   */
  def explainCell(query: Operator, token: RowIdPrimitive, column: String): CellExplanation =
    explainer.explainCell(query, token, column)

  /**
   * Validate that the specified operator is valid
   */
  def check(oper: Operator): Unit =
    Typechecker.schemaOf(oper);

  /**
   * Parse the provided string as a Mimir Expression AST
   */
  def parseExpression(exprString: String): Expression =
    operator.expr(exprString)
  /**
   * Parse the provided string as a list of comma-delimited Mimir Expression ASTs
   */
  def parseExpressionList(exprListString: String): List[Expression] =
    operator.exprList(exprListString)
  /**
   * Parse the provided string as a Mimir RA AST
   */
  def parseOperator(operString: String): Operator =
    operator.operator(operString)

  /**
   * Get all availale table names 
   */
  def getAllTables(): Set[String] =
  {
    (
      backend.getAllTables() ++ views.list()
    ).toSet[String];
  }

  /**
   * Determine whether the specified table exists
   */
  def tableExists(name: String): Boolean =
  {
    getTableSchema(name) != None
  }

  /**
   * Look up the schema for the table with the provided name.
   */
  def getTableSchema(name: String): Option[Seq[(String,Type)]] = {
    logger.debug(s"Table schema for $name")
    getView(name).map(_.schema).
      orElse(backend.getTableSchema(name))
  }

  /**
   * Build a Table operator for the table with the provided name.
   */
  def getTableOperator(table: String): Operator =
  {
    getView(table).getOrElse(
      Table(
        table, 
        backend.getTableSchema(table) match {
          case Some(x) => x
          case None => throw new SQLException(s"No such table or view '$table'")
        },
        Nil
      ) 
    )
  }

  /**
   * Evaluate a statement that does not produce results.
   *
   * Generally these are routed directly to the back-end, but there
   * are a few operations that Mimir needs to handle directly.
   */
  def update(stmt: Statement)
  {
    stmt match {
      /********** QUERY STATEMENTS **********/
      case _: Select   => throw new SQLException("Can't evaluate SELECT as an update")
      case _: Explain  => throw new SQLException("Can't evaluate EXPLAIN as an update")
      case _: Pragma   => throw new SQLException("Can't evaluate PRAGMA as an update")
      case _: Analyze  => throw new SQLException("Can't evaluate ANALYZE as an update")

      /********** FEEDBACK STATEMENTS **********/
      case feedback: Feedback => {
        val name = feedback.getModel().toUpperCase()
        val idx = feedback.getIdx()
        val args = feedback.getArgs().map(sql.convert(_))
        val v = sql.convert(feedback.getValue())

        val model = models.get(name) 
        model.feedback(idx, args, v)
        models.persist(model)
        if(!backend.canHandleVGTerms){
          bestGuessCache.update(model, idx, args, v)
        }
      }

      /********** CREATE LENS STATEMENTS **********/
      case lens: CreateLens => {
        val t = lens.getType().toUpperCase()
        val name = lens.getName()
        val query = sql.convert(lens.getSelectBody())
        val args = lens.getArgs().map(sql.convert(_, x => x)).toList

        lenses.create(t, name, query, args)
      }

      /********** CREATE VIEW STATEMENTS **********/
      case view: CreateView => {
        val viewName = view.getTable().getName().toUpperCase
        val baseQuery = sql.convert(view.getSelectBody())
        val optQuery = Compiler.optimize(baseQuery)

        views.create(viewName, optQuery);
      }

      /********** CREATE ADAPTIVE SCHEMA **********/
      case createAdaptive: CreateAdaptiveSchema => {
        adaptiveSchemas.create(
          createAdaptive.getName.toUpperCase,
          createAdaptive.getType.toUpperCase,
          sql.convert(createAdaptive.getSelectBody()),
          createAdaptive.getArgs.map( sql.convert(_, x => x) )
        )
      }

      /********** LOAD STATEMENTS **********/
      case load: Load => {
        // Assign a default table name if needed
        val (target, force) = 
          load.getTable() match { 
            case null => (load.getFile.getName.replaceAll("\\..*", "").toUpperCase, false)
            case s => (s, true)
          }

        loadTable(target, load.getFile, force = force)
      }

      /********** DROP STATEMENTS **********/
      case drop: Drop     => {
        drop.getType().toUpperCase match {
          case "TABLE" | "INDEX" =>
            backend.update(drop.toString());
            backend.invalidateCache();

          case "VIEW" =>
            views.drop(drop.getName().toUpperCase);

          case "LENS" =>
            lenses.drop(drop.getName().toUpperCase)

          case _ =>
            throw new SQLException("Invalid drop type '"+drop.getType()+"'")
        }
      }

      /********** ALTER STATEMENTS **********/
      case alter: AlterViewMaterialize => {
        if(alter.getDrop){
          views.dematerialize(alter.getTarget.toUpperCase)
        } else {
          views.materialize(alter.getTarget.toUpperCase)
        } 
      }

      case _                => backend.update(stmt.toString())
    }
  }
  
  /**
   * Prepare a database for use with Mimir.
   */
  def initializeDBForMimir(): Unit = {
    models.init()
    views.init()
    lenses.init()
    adaptiveSchemas.init()
  }

  /**
   * Retrieve the query corresponding to the Lens or Virtual View with the specified
   * name (or None if no such lens exists)
   */
  def getView(name: String): Option[(Operator)] =
    catalog(name).orElse(
      views.get(name).map(_.operator)
    )

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

  def loadTable(targetTable: String, sourceFile: File, force:Boolean = true){
    val targetRaw = targetTable.toUpperCase + "_RAW"
    if(tableExists(targetRaw) && !force){
      throw new SQLException(s"Target table $targetTable already exists; Use `LOAD 'file' AS tableName`; to override.")
    }
    LoadCSV.handleLoadTable(this, targetRaw, sourceFile)
    val oper = getTableOperator(targetRaw)
    val l = List(new FloatPrimitive(.5))

    lenses.create("TYPE_INFERENCE", targetTable.toUpperCase, oper, l)
  }
  
  def loadTable(targetTable: String, sourceFile: String){
    loadTable(targetTable, new File(sourceFile))
  }
  def loadTable(sourceFile: String){
    loadTable(new File(sourceFile))
  }
  def loadTable(sourceFile: File){
    loadTable(sourceFile.getName().split("\\.")(0), sourceFile)
  }

  /**
    * Materialize a view into the database
    */
  def selectInto(targetTable: String, sourceQuery: Operator){
    val tableSchema = sourceQuery.schema
    val tableDef = tableSchema.map( x => x._1+" "+Type.toString(x._2) ).mkString(",")
    val tableCols = tableSchema.map( _._1 ).mkString(",")
    val colFillIns = tableSchema.map( _ => "?").mkString(",")
    backend.update(  s"CREATE TABLE $targetTable ( $tableDef );"  )
    val insertCmd = s"INSERT INTO $targetTable( $tableCols ) VALUES ($colFillIns);"
    println(insertCmd)
    query(sourceQuery) { result =>
      backend.fastUpdateBatch(
        insertCmd,
        result.map( _.tuple ) 
      )
    }
  }
  

  def selectInto(targetTable: String, tableName: String): Unit =
  {
/*    val v:Option[Operator] = getView(tableName)
    val mod:Model = models.getModel(tableName)
    mod.bestGuess()
    v match {
      case Some(o) => println("Schema: " + o.schema.toString())
      case None => println("Not a View")
    }
*/
    val tableS = this.getTableSchema(tableName)
    var tableDef = ""
    var tableCols = ""
    var colFillIns = ""
    tableS match {
      case Some(tableSchema) => {
        tableDef = tableSchema.map( x => x._1+" "+Type.toString(x._2) ).mkString(",")
        tableCols = tableSchema.map( _._1 ).mkString(",")
        colFillIns = tableSchema.map( _ => "?").mkString(",")
      }
      case None => throw new SQLException
    }
    println(  s"CREATE TABLE $targetTable ( $tableDef );"  )
    backend.update(  s"CREATE TABLE $targetTable ( $tableDef );"  )
    val insertCmd = s"INSERT INTO $targetTable( $tableCols ) VALUES ($colFillIns);"
    println(insertCmd)
    query("SELECT * FROM " + tableName + ";")(_.foreach { result =>
      backend.update(insertCmd, result.tuple)
    })
  }
  def select(s: String) = 
  {
    this.sql.convert(stmt(s).asInstanceOf[net.sf.jsqlparser.statement.select.Select])
  }
  def stmt(s: String) = {
    new MimirJSqlParser(new StringReader(s)).Statement()
  }

  /**
   * Utility for modules to ensure that a table with the specified schema exists.
   *
   * If the table doesn't exist, it will be created.
   * If the table does exist, non-existant columns will be created.
   * If the table does exist and a column has a different type, an error will be thrown.
   */
  def requireTable(name: String, schema: Seq[(String, Type)], primaryKey: Option[String] = None)
  {
    val typeMap = schema.map { x => (x._1.toUpperCase -> x._2) }.toMap
    backend.getTableSchema(name) match {
      case None => {
        val schemaElements = 
          schema.map { case (name, t) => s"$name $t" } ++ 
          (if(primaryKey.isEmpty) { Seq() } else {
            Seq(s"PRIMARY KEY (${primaryKey.get})")
          })
        val createCmd = s"""
          CREATE TABLE $name(
            ${schemaElements.mkString(",\n            ")}
          )
        """
        logger.debug(s"CREATE: $createCmd")
        backend.update(createCmd);
      }
      case Some(oldSch) => {
        val currentColumns = oldSch.map { _._1 }.toSet
        for(column <- (typeMap.keySet ++ currentColumns)){
          if(typeMap contains column){
            if(!(currentColumns contains column)){
              logger.debug("Need to add $column to $name(${typemap.keys.mkString(", ")})")
              backend.update(s"ALTER TABLE $name ADD COLUMN $column ${typeMap(column)}")
            }
          }
        }
      }

    }
  }
}