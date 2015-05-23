package mimir;

import java.sql._;

import mimir.sql.{Backend,SqlToRA,RAToSql,CreateLens};
import mimir.exec.{Compiler,ResultIterator,ResultSetIterator};
import mimir.ctables.{VGTerm,CTPercolator,Model};
import mimir.lenses.{Lens,LensManager}
import mimir.parser.{OperatorParser}
import mimir.algebra._;


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
case class Database(backend: Backend)
{
  val sql = new SqlToRA(this)
  val ra = new RAToSql(this)
  val lenses = new LensManager(this)
  val compiler = new Compiler(this)  
  val operator = new OperatorParser(this.getLensModel, this.getTableSchema(_).toMap)
  
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
    // println(sql);
    backend.update(sql);
  }
  /**
   * Evaluate the specified SQL DDL expression on the backend directly.  JDBC 
   * parameters (`?`) are replaced according to the provided argument list.
   * No Mimir-specific optimizations or updates are applied.
   */
  def update(sql: String, args: List[String]): Unit = {
    // println(sql);
    backend.update(sql, args);
  }

  /** 
   * Apply the standard set of Mimir compiler optimizations -- Used mostly for EXPLAIN.
   */
  def optimize(oper: Operator): Operator =
  {
    compiler.standardOptimizations.foldLeft(oper)( (o, fn) => fn(o) )
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
    if(result.missingRows){
      println("( There may be missing result rows )")
    }
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
  def getTableSchema(name: String): List[(String,Type.T)] =
    backend.getTableSchema(name);  
  /**
   * Build a Table operator for the table with the provided name.
   */
  def getTableOperator(table: String): Operator =
    backend.getTableOperator(table)
  /**
   * Build a Table operator for the table with the provided name, requesting the
   * specified metadata.
   */
  def getTableOperator(table: String, metadata: Map[String, Type.T]): Operator =
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
    lenses.init();
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
      lenses.load(name.toUpperCase()) match {
        case None => None
        case Some(lens) => 
          // println("Found: "+name); 
          Some(lens.view)
      }
    }
  }
}