package mimir

import java.io.File
import java.io.StringReader
import java.sql.SQLException
import java.sql.ResultSet

import mimir.algebra._
import mimir.ctables.{CTExplainer, CTPercolator, CellExplanation, RowExplanation, InlineVGTerms}
import mimir.models.Model
import mimir.exec.Compiler
import mimir.exec.mode.{CompileMode, BestGuess}
import mimir.exec.result.{ResultIterator,SampleResultIterator,Row}
import mimir.lenses.{LensManager}
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
import mimir.optimizer.operator.OptimizeExpressions
import mimir.util.{LoadCSV,ExperimentalOptions}
import mimir.parser.MimirJSqlParser
import mimir.statistics.FuncDep

import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.select.Select
import net.sf.jsqlparser.statement.create.table.CreateTable
import net.sf.jsqlparser.statement.drop.Drop

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
case class Database(backend: Backend)
  extends LazyLogging
{
  //// Persistence
  val lenses          = new mimir.lenses.LensManager(this)
  val models          = new mimir.models.ModelManager(this)
  val views           = new mimir.views.ViewManager(this)
  val adaptiveSchemas = new mimir.adaptive.AdaptiveSchemaManager(this)

  //// Parsing & Reference
  val sql             = new mimir.sql.SqlToRA(this)
  val ra              = new mimir.sql.RAToSql(this)
  val functions       = new mimir.algebra.function.FunctionRegistry
  val aggregates      = new mimir.algebra.function.AggregateRegistry

  //// Logic
  val compiler        = new mimir.exec.Compiler(this)
  val explainer       = new mimir.ctables.CTExplainer(this)
  val catalog         = new mimir.statistics.SystemCatalog(this)
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
    val iterator = mode(this, oper)
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
  final def query[T, R <:ResultIterator](stmt: net.sf.jsqlparser.statement.select.Select, mode: CompileMode[R])(handler: R => T): T =
    query(sql.convert(stmt), mode)(handler)

  /**
   * Translate, optimize and evaluate the specified query.  Applies all Mimir-specific 
   * optimizations and rewrites the query to properly account for Virtual Tables.
   */
  final def query[T, R <:ResultIterator](stmt: String, mode: CompileMode[R])(handler: R => T): T =
    query(select(stmt), mode)(handler)

  /**
   * Optimize and evaluate the specified query.  Applies all Mimir-specific optimizations
   * and rewrites the query to properly account for Virtual Tables.
   */
  final def query[T](oper: Operator)(handler: ResultIterator => T): T =
    query(oper, BestGuess)(handler)

  /**
   * Translate, optimize and evaluate the specified query.  Applies all Mimir-specific 
   * optimizations and rewrites the query to properly account for Virtual Tables.
   */
  final def query[T](stmt: net.sf.jsqlparser.statement.select.Select)(handler: ResultIterator => T): T = 
    query(stmt, BestGuess)(handler)

  /**
   * Translate, optimize and evaluate the specified query.  Applies all Mimir-specific 
   * optimizations and rewrites the query to properly account for Virtual Tables.
   */
  final def query[T](stmt: String)(handler: ResultIterator => T): T = 
    query(select(stmt), BestGuess)(handler)


  /**
   * Make an educated guess about what the query's schema should be
   * XXX: Only required because the TypeInference lens is a little punk birch that needs to
   *      be converted into an adaptive schema (#193).
   */
  def bestGuessSchema(oper: Operator): Seq[(String, Type)] =
  {
    val Inline = new OptimizeExpressions(compiler.optimize(_))
    typechecker.schemaOf(Inline(InlineVGTerms(oper, this)))
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
    tableSchema(name) != None
  }

  /**
   * Look up the schema for the table with the provided name.
   */
  def tableSchema(name: String): Option[Seq[(String,Type)]] = {
    logger.debug(s"Table schema for $name")
    views.get(name) match { 
      case Some(viewDefinition) => Some(viewDefinition.schema)
      case None => backend.getTableSchema(name)
    }
  }

  /**
   * Build a Table operator for the table with the provided name.
   */
  def table(tableName: String) : Operator = table(tableName, tableName)
  def table(tableName: String, alias:String): Operator =
  {
    getView(tableName).getOrElse(
      Table(
        tableName, alias,
        backend.getTableSchema(tableName) match {
          case Some(x) => x
          case None => throw new SQLException(s"No such table or view '$tableName'")
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
        val args = feedback.getArgs().map { sql.convert(_) }.map { _.asString }.map { RowIdPrimitive(_) }
        val v = sql.convert(feedback.getValue())

        val model = models.get(name) 
        model.feedback(idx, args, v)
        models.persist(model)
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
        val optQuery = compiler.optimize(baseQuery)

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

        loadTable(
          target, 
          load.getFile, 
          force = force,
          (load.getFormat, load.getFormatArgs.asScala.toSeq.map { sql.convert(_) })
        )
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
  def loadTable(
    targetTable: String, 
    sourceFile: File, 
    force:Boolean = true, 
    format:(String, Seq[PrimitiveValue]) = ("CSV", Seq(StringPrimitive(",")))
  ){
    (format._1 match {
           case null => "CSV"
           case x => x.toUpperCase
     }) match {
      case "CSV" => {
        val (delim, typeinference, adaptive) =
          format._2 match {
            case Seq(StringPrimitive(delim_)) => (delim_, true, true)
            case Seq(StringPrimitive(delim_),BoolPrimitive(typeinference_)) => (delim_, typeinference_, true)
            case Seq(StringPrimitive(delim_),BoolPrimitive(typeinference_),BoolPrimitive(adaptive_)) => (delim_, typeinference_, adaptive_)
            case Seq() | null => (",", true, true)
            case _ => throw new SQLException("The CSV format expects a single string argument (CSV('delim'))")
          }

          val targetRaw = targetTable.toUpperCase + "_RAW"
          if(tableExists(targetRaw) && !force){
            throw new SQLException(s"Target table $targetTable already exists; Use `LOAD 'file' INTO tableName`; to append to existing data.")
          }
          if(!tableExists(targetTable.toUpperCase)){
            LoadCSV.handleLoadTableRaw(this, targetRaw, sourceFile, 
              Map("DELIMITER" -> delim)
            )
            val oper = table(targetRaw)
            //detect headers and create adaptive schema
            if(adaptive)
              adaptiveSchemas.create( targetTable.toUpperCase+"_DH", "DETECT_HEADER", oper, Seq())
            //create TI adaptive schema - also create a view with the target name 
            if(typeinference){
              adaptiveSchemas.create( targetTable.toUpperCase+"_TI", "TYPE_INFERENCE", adaptiveSchemas.viewFor(targetTable.toUpperCase+ "_DH", targetRaw).getOrElse(oper), Seq(FloatPrimitive(.5))) 
              views.create(targetTable.toUpperCase, adaptiveSchemas.viewFor(targetTable.toUpperCase+ "_TI", targetRaw).get)
            }
            else if(adaptive){
              //if there is no ti then make a view with the target name of the detect header adaptive schema view 
              views.create(targetTable.toUpperCase, adaptiveSchemas.viewFor(targetTable.toUpperCase+ "_DH", targetRaw).getOrElse(oper))
            }
            else {
              //if there is no ti or detect headers then make a view with the target name of the raw table
              views.create(targetTable.toUpperCase, oper)
            }
          } else LoadCSV.handleLoadTableRaw(this, targetTable.toUpperCase, sourceFile,  Map("DELIMITER" -> delim) )
        }
      case fmt =>
        throw new SQLException(s"Unknown load format '$fmt'")
    }
  }

  def loadTableNoTI(targetTable: String, sourceFile: File, force:Boolean = true): Unit ={
    if(tableExists(targetTable) && !force){
      throw new SQLException(s"Target table $targetTable already exists; Use `LOAD 'file' AS tableName`; to override.")
    }
    LoadCSV.handleLoadTable(this, targetTable, sourceFile)
  }

  def loadTableNoTI(targetTable: String, sourceFile: String){
    loadTableNoTI(targetTable, new File(sourceFile))
  }
  def loadTableNoTI(sourceFile: String){
    loadTableNoTI(new File(sourceFile))
  }
  def loadTableNoTI(sourceFile: File){
    loadTableNoTI(sourceFile.getName().split("\\.")(0), sourceFile)
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
    val tableSchema = typechecker.schemaOf(sourceQuery)
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
    val tableS = this.tableSchema(tableName)
    var tableDef = ""
    var tableCols = ""
    var colFillIns = ""
    tableS match {
      case Some(sch) => {
        tableDef = sch.map( x => x._1+" "+Type.toString(x._2) ).mkString(",")
        tableCols = sch.map( _._1 ).mkString(",")
        colFillIns = sch.map( _ => "?").mkString(",")
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
