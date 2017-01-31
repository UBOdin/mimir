package mimir

import java.io.File
import java.io.StringReader
import java.sql.SQLException
import java.sql.ResultSet

import mimir.algebra._
import mimir.ctables.{CTExplainer, CTPercolator, CellExplanation, Model, RowExplanation, VGTerm}
import mimir.exec.{Compiler, ResultIterator, ResultSetIterator}
import mimir.lenses.{Lens, LensManager, BestGuessCache}
import mimir.parser.OperatorParser
import mimir.sql._
import mimir.util.LoadCSV
import mimir.web.WebIterator
import mimir.parser.MimirJSqlParser

import net.sf.jsqlparser.statement.Statement


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
  */
case class Database(name: String, backend: Backend)
{
  val sql = new SqlToRA(this)
  val ra = new RAToSql(this)
  val lenses = new LensManager(this)
  val compiler = new Compiler(this)
  val explainer = new CTExplainer(this)
  val bestGuessCache = new BestGuessCache(this)
  val operator = new OperatorParser(this.getLensModel,
    (x) => 
      this.getTableSchema(x) match {
        case Some(x) => x
        case None => throw new SQLException("Table "+x+" does not exist in db!")
      })

  def getName = name

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
    println(result.schema.map( _._1 ).mkString(","))
    println("------")

    while(result.getNext()){
      println(
        (0 until result.numCols).map( (i) => {
          if( i == 0 ){
            result(i) match {
              case NullPrimitive() => "'NULL'"
              case _ => result(i)
            }

          }
          else{
            result(i)+(
              if(!result.deterministicCol(i)){ "*" } else { "" }
              )
          }

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

//    isNullCheck()

  }

  def isNullCheck(): Unit ={
    if(IsNullChecker.getIsNull()){ // is NULL is in there so check

      if(IsNullChecker.getDB() == null){
        IsNullChecker.setDB(this)
      }

      if(IsNullChecker.isNullCheck()){

      }
      else{
        println("IS NULL HAS PROBLEMS")
        IsNullChecker.problemRows();
      }

    }
    IsNullChecker.reset()
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
  def getTableOperator(table: String, metadata: List[(String, Expression, Type.T)]): Operator =
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
          Some(lens.view)
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

  def loadTable(targetTable: String, sourceFile: File){
    LoadCSV.handleLoadTable(this, targetTable, sourceFile)
//    val l = List(new FloatPrimitive(.5))

//    lenses.create(oper, targetTable, l,"TYPE_INFERENCE")
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