package mimir

import java.sql.SQLException

import mimir.algebra._
import mimir.ctables.{Model, VGTerm, CTPercolator}
import mimir.exec.{Compiler, NonDeterminism, ResultIterator, ResultSetIterator}
import mimir.lenses.{Lens, LensManager}
import mimir.parser.OperatorParser
import mimir.sql.{Backend, CreateLens, RAToSql, SqlToRA}
import mimir.util.LoadCSV
import mimir.web.WebIterator

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
  * - mimir.sql.SqlToRA (sql)
  *    Responsible for translating JSqlParser AST elements into corresponding AST elements from
  *    mimir.algebra._  
  * - mimir.sql.RAToSql (ra)
  *    Responsible for translating mimir.algebra._ AST elements back to JSqlParser's AST.  This is
  *    typically only required for compatibility with JDBC.
  * - mimir.parser.OperatorParser (operator)
  *    Responsible for directly constructing mimir.algebra.{Operator,Expression} ASTs from string
  *    representations.  Allows these ASTs to be serialized through toString()
  *
  * === Logic ===
  * - mimir.sql.Backend (backend)
  *    Pluggable wrapper for database backends over which Mimir will actually run.  Basically,
  *    a simplified form of JDBC.  See mimir.sql._ for examples.
  * - mimir.lenses.LensManager (lenses)
  *    Responsible for creating, serializing, and deserializing lenses and virtual views.
  * - mimir.exec.Compiler
  *    Responsible for query execution.  Acts as a wrapper around the logic in mimir.ctables._, 
  *    mimir.lenses._, and mimir.exec._ that prepares non-deterministic queries to be evaluated
  *    on the backend database.  
  */
case class Database(name: String, backend: Backend)
{
  val sql = new SqlToRA(this)
  val ra = new RAToSql(this)
  val lenses = new LensManager(this)
  val compiler = new Compiler(this)  
  val operator = new OperatorParser(this.getLensModel,
    (x) => 
      this.getTableSchema(x) match {
        case Some(x) => x
        case None => throw new SQLException("Table "+x+" does not exist in db!")
      })
  var nonDeterminismStrategy = NonDeterminism.Hybrid

  def getName = name
  
  /**
   * Evaluate the specified query on the backend directly and wrap the result in a
   * ResultSetIterator.  No Mimir-specific optimizations or rewrites are applied.
   */
  def query(sql: String): ResultIterator = 
    new ResultSetIterator(backend.execute(sql))
  /**
   * Evaluate the specified query on the backend directly and wrap the result in a
   * ResultSetIterator.  JDBC parameters (`?`) are replaced according to the provided
   * argument list.  No Mimir-specific optimizations or rewrites are applied.
   */
  def query(sql: String, args: List[String]): ResultIterator = 
    new ResultSetIterator(backend.execute(sql, args))
  /**
   * Evaluate the specified query on the backend directly.  No Mimir-specific 
   * optimizations or rewrites are applied.
   */
  def query(sql: net.sf.jsqlparser.statement.select.Select): ResultIterator = 
    new ResultSetIterator(backend.execute(sql))
  /**
   * Evaluate the specified query on the backend directly.  No Mimir-specific 
   * optimizations or rewrites are applied.
   */
  def query(sql: net.sf.jsqlparser.statement.select.SelectBody): ResultIterator =
    new ResultSetIterator(backend.execute(sql))
  
  /**
   * Evaluate the specified SQL DDL expression on the backend directly.  No Mimir-
   * specific optimizations or updates are applied.
   */
  def update(sql: String): Unit = {
    backend.update(sql)
  }

  /**
   * Evaluate a list of SQL statements in batch mode. This is useful for speeding up
   * data insertion during CSV uploads
   */
  def update(sql: List[String]): Unit = {
    backend.update(sql)
  }
  /**
   * Evaluate the specified SQL DDL expression on the backend directly.  JDBC 
   * parameters (`?`) are replaced according to the provided argument list.
   * No Mimir-specific optimizations or updates are applied.
   */
  def update(sql: String, args: List[String]): Unit = {
    backend.update(sql, args)
  }

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
   * Optimize and evaluate the specified query.  Applies all Mimir-specific optimizations
   * and rewrites the query to properly account for Virtual Tables.
   */
  def queryLineage(oper: Operator): ResultIterator = 
  {
    compiler.compile(oper, NonDeterminism.Classic)
  }

  /**
   * Flush the provided ResultIterator to the console.
   */
  def dump(result: ResultIterator): Unit =
  {
    println(result.schema.map( _._1 ).mkString(","))
    println("------")
    while(result.getNext()){
      println(
        (0 until result.numCols).map( (i) => {
          result(i)+(
            if(!result.deterministicCol(i)){ "*" } else { "" }
            )
        }).mkString(",")+(
          if(!result.deterministicRow){
            " (This row may be invalid)"
          } else { "" }
          )
      )
    }
    if(result.missingRows()){
      println("( There may be missing result rows )")
    }
  }

  /**
   * Construct a WebIterator from a ResultIterator
   */
  def generateWebIterator(result: ResultIterator): WebIterator =
  {
    val startTime = System.nanoTime()

    val headers: List[String] = result.schema.map(_._1)
    val data: ListBuffer[(List[String], Boolean)] = new ListBuffer()

    var i = 0
    while(result.getNext()){
      val list =
        (0 until result.numCols).map( (i) => {
          result(i) + (if (!result.deterministicCol(i)) {"*"} else {""})
        }).toList

//      println("RESULTS: "+list)
      if(i < 100) data.append((list, result.deterministicRow()))
      i = i + 1
    }

    val executionTime = (System.nanoTime() - startTime) / (1 * 1000 * 1000)
    new WebIterator(headers, data.toList, i, result.missingRows(), executionTime)
  }

  /**
   * Translate the specified JSqlParser SELECT statement to Mimir's RA AST.
   */
  def convert(sel: net.sf.jsqlparser.statement.select.Select): Operator =
    sql.convert(sel)
  /**
   * Translate the specified JSqlParser SELECT body to Mimir's RA AST.
   */
  def convert(sel: net.sf.jsqlparser.statement.select.SelectBody): (Operator,Map[String, String]) =
    sql.convert(sel, null)
  /**
   * Translate the specified JSqlParser expression to Mimir's Expression AST.
   */
  def convert(expr: net.sf.jsqlparser.expression.Expression): Expression =
    sql.convert(expr)
  /**
   * Translate the specified Mimir RA AST back to a JSqlParser statement.
   */
  def convert(oper: Operator): net.sf.jsqlparser.statement.select.SelectBody =
    ra.convert(oper)

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
   * Look up the schema for the table with the provided name.
   */
  def getTableSchema(name: String): Option[List[(String,Type.T)]] =
    backend.getTableSchema(name)
  /**
   * Build a Table operator for the table with the provided name.
   */
  def getTableOperator(table: String): Operator =
    backend.getTableOperator(table)
  /**
   * Build a Table operator for the table with the provided name, requesting the
   * specified metadata.
   */
  def getTableOperator(table: String, metadata: List[(String, Type.T)]): Operator =
    backend.getTableOperator(table, metadata)
  
  /**
   * Evaluate a CREATE LENS statement.
   */
  def createLens(lensDefn: CreateLens): Unit =
    lenses.create(lensDefn)
  
  /**
   * Prepare a database for use with Mimir.
   */
  def initializeDBForMimir(): Unit = {
    lenses.init()
  }

  /**
   * Retrieve the Lens with the specified name.
   */
  def getLens(lensName: String): Lens =
    lenses.load(lensName).get
  /**
   * Retrieve the Model for the Lens with the specified name.
   */
  def getLensModel(lensName: String): Model = 
    lenses.modelForLens(lensName)  
  /**
   * Retrieve the query corresponding to the Lens or Virtual View with the specified
   * name (or None if no such lens exists)
   */
  def getView(name: String): Option[(Operator)] =
  {
    // System.out.println("Selecting from ..."+name);
    if(lenses == null){ None }
    else {
      //println(iviews.views.toString())
      lenses.load(name.toUpperCase) match {
        case None => None
        case Some(lens) => 
          // println("Found: "+name); 
          Some(CTPercolator.propagateRowIDs(lens.view))
      }
    }
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
  def handleLoadTable(targetTable: String, sourceFile: String){
    LoadCSV.handleLoadTable(this, targetTable, sourceFile)
  }

  /**
   * Find all VGTerms in an expression
   */
  def getVGTerms(expression: Expression): List[VGTerm] = {
    Eval.getVGTerms(expression)
  }

  /**
   * Find all VGTerms in an expression
   */
  def getVGTerms(expression: Expression,
                 bindings: Map[String, PrimitiveValue],
                 list: List[VGTerm]): List[VGTerm] = {

    Eval.getVGTerms(expression, bindings, list)
  }

  def getBackendSQL(o: Operator): List[net.sf.jsqlparser.statement.select.SelectBody] =
  {
    o match {
      case Project(_, src) => getBackendSQL(src)
      case Union(lhs,rhs) => getBackendSQL(lhs)++getBackendSQL(rhs);
      case _ => List(convert(o))

    }
  }
}