package mimir

import java.io.File
import java.io.StringReader
import java.sql.SQLException
import java.sql.ResultSet

import mimir.algebra._
import mimir.ctables.{CTExplainer, CTPercolator, CellExplanation, RowExplanation, VGTerm}
import mimir.models.Model
import mimir.exec.{Compiler, ResultIterator, ResultSetIterator}
import mimir.lenses.{LensManager, BestGuessCache}
import mimir.parser.OperatorParser
import mimir.sql.{SqlToRA,RAToSql,Backend,CreateLens,CreateView,Explain,Feedback,Load,Pragma,Analyze,CreateAdaptiveSchema}
import mimir.optimizer.{InlineVGTerms, ResolveViews}
import mimir.util.{LoadCSV,ExperimentalOptions}
import mimir.web.WebIterator
import mimir.parser.MimirJSqlParser
import mimir.statistics.FuncDep

import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.select.Select
import net.sf.jsqlparser.statement.create.table.CreateTable
import net.sf.jsqlparser.statement.drop.Drop

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
  * 
  * === Logic ===
  * * mimir.sql.Backend (backend)
  *    Pluggable wrapper for database backends over which Mimir will actually run.  Basically,
  *    a simplified form of JDBC.  See mimir.sql._ for examples.
  * * mimir.lenses.LensManager (lenses)
  *    Responsible for creating, serializing, and deserializing lenses and virtual views.
  * * mimir.exec.Compiler
  *    Responsible for query execution.  Acts as a wrapper around the logic in mimir.ctables._, 
  *    mimir.lenses._, and mimir.exec._ that prepares non-deterministic queries to be evaluated
  *    on the backend database.  
  * * mimir.explainer.CTExplainer (explainer)
  *    Responsible for creating explanation objects.
  */
case class Database(backend: Backend)
{
  //// Persistence
  val lenses          = new mimir.lenses.LensManager(this)
  val models          = new mimir.models.ModelManager(this)
  val views           = new mimir.views.ViewManager(this)
  val bestGuessCache  = new mimir.lenses.BestGuessCache(this)
  val querySerializer = new mimir.algebra.Serialization(this)

  //// Logic
  val compiler        = new mimir.exec.Compiler(this)
  val explainer       = new mimir.ctables.CTExplainer(this)

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
    compiler.optimize(oper)
  }

  /**
   * Optimize and evaluate the specified query.  Applies all Mimir-specific optimizations
   * and rewrites the query to properly account for Virtual Tables.
   */
  def query(oper: Operator): ResultIterator = 
  {
    compiler.compile(oper)
  }

  /**
   * Translate, optimize and evaluate the specified query.  Applies all Mimir-specific 
   * optimizations and rewrites the query to properly account for Virtual Tables.
   */
  def query(stmt: net.sf.jsqlparser.statement.select.Select): ResultIterator = 
  {
    query(sql.convert(stmt))
  }

  /**
   * Make an educated guess about what the query's schema should be
   */
  def bestGuessSchema(oper: Operator): Seq[(String, Type)] =
  {
    InlineVGTerms(ResolveViews(this, oper)).schema
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
   * Flush the provided ResultIterator to the console.
   */
  def dump(result: ResultIterator): Unit =
  {
    ExperimentalOptions.ifEnabled("SILENT-TEST", () => {
      var x = 0
      while(result.getNext()){ x += 1; if(x % 10000 == 0) {println(s"$x rows")} }
      val missingRows = result.missingRows()
      println(s"Total $x rows; Missing: $missingRows")
    }, () => {
      println(result.schema.map( _._1 ).mkString(","))
      println("------")
      result.foreachRow { row => 
        println(
          (0 until row.numCols).map { i => 
            row(i)+(
              if(!row.deterministicCol(i)){ "*" } else { "" }
            )
          }.mkString(",")+(
            if(!row.deterministicRow){
              " (This row may be invalid)"
            } else { "" }
          )
        )
      }
      if(result.missingRows()){
        println("( There may be missing result rows )")
      }
    })

  }

  /**
   * Construct a WebIterator from a ResultIterator
   */
  def generateWebIterator(result: ResultIterator): WebIterator =
  {
    val startTime = System.nanoTime()

    // println("SCHEMA: "+result.schema)
    val headers: List[String] = "MIMIR_ROWID" :: result.schema.map(_._1).toList
    val data: ListBuffer[(List[String], Boolean)] = new ListBuffer()

    var i = 0
    while(result.getNext()){
      val list =
        (
          result.provenanceToken().payload.toString ::
            result.schema.zipWithIndex.map( _._2).map( (i) => {
              result(i).toString + (if (!result.deterministicCol(i)) {"*"} else {""})
            }).toList
        )

     // println("RESULTS: "+list)
      if(i < 100) data.append((list, result.deterministicRow()))
      i = i + 1
    }

    val executionTime = (System.nanoTime() - startTime) / (1 * 1000 * 1000)
    new WebIterator(headers, data.toList, i, result.missingRows(), executionTime)
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
   * Look up the schema for the table with the provided name.
   */
  def getTableSchema(name: String): Option[Seq[(String,Type)]] =
    getView(name).map(_.schema).
      orElse(backend.getTableSchema(name))

  /**
   * Build a Table operator for the table with the provided name.
   */
  def getTableOperator(table: String): Operator =
    getTableOperator(table, Nil)

  /**
   * Build a Table operator for the table with the provided name, requesting the
   * specified metadata.
   */
  def getTableOperator(table: String, metadata: Seq[(String, Expression, Type)]): Operator =
  {
    Table(
      table, 
      getTableSchema(table) match {
        case Some(x) => x
        case None => throw new SQLException("Table does not exist in db!")
      },
      metadata
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
        models.update(model)
        if(!backend.canHandleVGTerms()){
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
      case view: CreateView => views.create(view.getTable().getName().toUpperCase, 
                                             sql.convert(view.getSelectBody()))

      /********** CREATE ADAPTIVE SCHEMA **********/
      case adaptiveSchema: CreateAdaptiveSchema => {
        val fdStats = new FuncDep()
        val query = sql.convert(adaptiveSchema.getSelectBody())
        val schema : Seq[(String,Type)] = query.schema

        val viewList : java.util.ArrayList[String] = 
          fdStats.buildEntities(
            schema, 
            backend.execute(adaptiveSchema.getSelectBody.toString()), 
            adaptiveSchema.getTable.getName
          )
        viewList.foreach((view) => {
          println(view)
          // update(view)
        })      
      }

      /********** LOAD STATEMENTS **********/
      case load: Load => {
        // Assign a default table name if needed
        val target = 
          load.getTable() match { 
            case null => load.getFile.getName.replaceAll("\\..*", "").toUpperCase
            case s => s
          }

        loadTable(target, load.getFile)
      }

      /********** DROP STATEMENTS **********/
      case drop: Drop     => {
        drop.getType().toUpperCase match {
          case "TABLE" | "INDEX" => 
            backend.update(drop.toString());

          case "VIEW" =>
            views.drop(drop.getName().toUpperCase);

          case "LENS" =>
            lenses.drop(drop.getName().toUpperCase)

          case _ =>
            throw new SQLException("Invalid drop type '"+drop.getType()+"'")
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
  }

  /**
   * Retrieve the query corresponding to the Lens or Virtual View with the specified
   * name (or None if no such lens exists)
   */
  def getView(name: String): Option[(Operator)] =
    views.get(name)

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

  def loadTable(targetTable: String, sourceFile: File){
    val targetRaw = targetTable.toUpperCase + "_RAW"
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
    query(sourceQuery).foreachRow( 
      result => 
        backend.update(insertCmd, result.currentRow())
    )
  }
}