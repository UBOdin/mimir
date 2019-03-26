package mimir

import java.io.File
import java.io.StringReader
import java.sql.SQLException
import java.sql.ResultSet
import java.net.URL

import sparsity.Name
import sparsity.statement._
import sparsity.expression.Expression

import mimir.algebra._
import mimir.ctables.{CTExplainer, CTPercolator, CellExplanation, RowExplanation, InlineVGTerms}
import mimir.models.Model
import mimir.exec.Compiler
import mimir.exec.mode.{CompileMode, BestGuess}
import mimir.exec.result.{ResultIterator,SampleResultIterator,Row}
import mimir.lenses.{LensManager}
import mimir.sql.{SqlToRA,RAToSql}
import mimir.backend.{QueryBackend,MetadataBackend}
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
import mimir.util.{LoadCSV,ExperimentalOptions}
import mimir.parser.MimirSQL
import mimir.statistics.FuncDep

import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import mimir.exec.result.JDBCResultIterator
import mimir.util.LoadData


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
case class Database(backend: QueryBackend, metadataBackend: MetadataBackend)
  extends LazyLogging
{
  //// Persistence
  val lenses          = new mimir.lenses.LensManager(this)
  val models          = new mimir.models.ModelManager(this)
  val views           = new mimir.views.ViewManager(this)
  val transientViews  = scala.collection.mutable.Map[ID, Operator]()
  val adaptiveSchemas = new mimir.adaptive.AdaptiveSchemaManager(this)

  //// Parsing & Reference
  val sqlToRA         = new mimir.sql.SqlToRA(this)
  val raToSQL         = new mimir.sql.RAToSql(this)
  val functions       = new mimir.algebra.function.FunctionRegistry()
  val aggregates      = new mimir.algebra.function.AggregateRegistry()

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
  val metadataTables = Seq(
    "MIMIR_ADAPTIVE_SCHEMAS", 
    "MIMIR_MODEL_OWNERS",
    "MIMIR_MODELS", 
    "MIMIR_VIEWS", 
    "MIMIR_SYS_TABLES", 
    "MIMIR_SYS_ATTRS"
  ).map { ID(_) }
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
   * Get all availale table names 
   */
  def getAllTables(): Set[ID] =
  {
    (
      catalog.list()
      ++ transientViews.keys
      ++ views.list()
      ++ backend.getAllTables()
    ).toSet[ID];
  }

  /**
   * Determine whether the specified table exists
   */
  def tableExists(name: Name): Boolean =
    if(name.quoted) { tableExists(ID(name.name)) }
    else { tableExists(name.name) }
  def tableExists(name: String): Boolean =
    tableSchema(name) != None
  def tableExists(name: ID): Boolean =
    tableSchema(name) != None

  /**
   * Look up the schema for the table with the provided name.
   */
  def tableSchema(name: Name): Option[Seq[(ID,Type)]] = 
    if(name.quoted) { tableSchema(ID(name.name)) }
    else { tableSchema(name.name) }
  def tableSchema(name: String): Option[Seq[(ID,Type)]] = 
    tableSchema(resolveCaseInsensitiveTable(name))
  def tableSchema(name: ID): Option[Seq[(ID,Type)]] = {
    logger.debug(s"Table schema for $name")
    transientViews.get(name) match {
      case Some(viewQuery) => return Some(typechecker.schemaOf(viewQuery))
      case None => ()
    }
    views.get(name) match {
      case Some(view) => return Some(view.schema)
      case None => ()
    }
    return backend.getTableSchema(name)
  }

  /** 
   * Finds the appropriate capitalization for a case-insensitive
   * table name.  If the table/view/etc... exists, return a 
   * case-sensitive reference to the table.  If it does not exist,
   * fall through to returning an upper-case version of the
   * name.
   */
  def resolveCaseInsensitiveTable(name: String, extras: Set[ID] = Set()): ID = 
    (extras ++ getAllTables)
       .find { _.id.equalsIgnoreCase(name) }
       .getOrElse { ID(name.toUpperCase) }

  /**
   * Build a Table operator for the table with the provided name.
   */
  def table(name: Name) : Operator = 
    if(name.quoted) { table(ID(name.name)) }
    else { table(name.name) }
  def table(name: Name, alias:ID) : Operator = 
    if(name.quoted) { table(ID(name.name), alias) }
    else { table(name.name, alias) }
  def table(tableName: String) : Operator = 
    table(resolveCaseInsensitiveTable(tableName), ID(tableName))
  def table(tableName: String, alias:ID) : Operator = 
    table(resolveCaseInsensitiveTable(tableName), alias)
  def table(tableName: ID) : Operator = 
    table(tableName, tableName)
  def table(tableName: ID, alias: ID): Operator =
  {
    transientViews.get(tableName).getOrElse {
      getView(tableName).getOrElse {
        Table(
          tableName, alias,
          backend.getTableSchema(tableName) match {
            case Some(x) => x
            case None => throw new SQLException(s"No such table or view '$tableName'")
          },
          Nil
        ) 
      }
    }
  }
  
  
  /**
   * Optimize and evaluate the specified query.  Applies all Mimir-specific optimizations
   * and rewrites the query to properly account for Virtual Tables.
   */
  final def queryMetadata[T, R <:ResultIterator](oper: Operator, mode: CompileMode[R])(handler: R => T): T =
  {
    val iterator = mode(this, oper, compiler.metadataBackendRootIterator)
    try {
      val ret = handler(iterator)
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
  final def queryMetadata[T](oper: Operator)(handler: ResultIterator => T): T = {
    queryMetadata(oper, BestGuess)(handler)
  }
    
  /**
   * get all metadata tables
   */
  def getAllMatadataTables(): Set[ID] =
  {
    (
      metadataBackend.getAllTables() ++ views.list()
    ).toSet[ID];
  }

  /**
   * Determine whether the specified table exists
   */
  def metadataTableExists(name: ID): Boolean =
  {
    metadataTableSchema(name) != None
  }

  /**
   * Look up the schema for the table with the provided name.
   */
  def metadataTableSchema(name: ID): Option[Seq[(ID,Type)]] = 
  {
    logger.debug(s"Table schema for $name")
    views.get(name) match { 
      case Some(viewDefinition) => Some(viewDefinition.schema)
      case None => metadataBackend.getTableSchema(name)
    }
  }

  /**
   * Build a Table operator for the table with the provided name.
   */
  def metadataTable(tableName: ID) : Operator = metadataTable(tableName, tableName)
  def metadataTable(tableName: ID, alias: ID): Operator = 
  {
    getView(tableName).getOrElse(
      Table(
        tableName, alias,
        metadataBackend.getTableSchema(tableName) match {
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

        views.create(
          ID.upper(view.name),
          optQuery
        )
      }

      /********** CREATE ADAPTIVE SCHEMA **********/
      case create: CreateAdaptiveSchema => {
        adaptiveSchemas.create(
          ID.upper(create.name),
          ID.upper(create.schemaType),
          sqlToRA(create.body),
          create.args.map( sqlToRA(_, sqlToRA.literalBindings(_)) )
        )
      }

      /********** LOAD STATEMENTS **********/
      case load: Load => {
        // Assign a default table name if needed
        loadTable(
          load.file, 
          targetTable = load.table.map { ID.upper(_) },
          force = (load.table != None),
          format = ID.lower(
                      load.format
                          .getOrElse { sparsity.Name("csv") }
                   ),
          loadOptions = load.args
                            .toMap
                            .mapValues { sqlToRA(_) }
                            .mapValues { _.asString }
        )
      }

      /********** DROP STATEMENTS **********/
      case SQLStatement(drop:DropTable) => {
        metadataBackend.update(drop.toString())
        metadataBackend.invalidateCache()
      }

      case SQLStatement(DropView(name, ifExists)) => views.drop(ID.upper(name), ifExists)
      case DropLens(name, ifExists)               => lenses.drop(ID.upper(name), ifExists)
      case DropAdaptiveSchema(name, ifExists)     => adaptiveSchemas.drop(ID.upper(name), ifExists)

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
  def initializeDBForMimir(): Unit = {
    models.init()
    views.init()
    lenses.init()
    adaptiveSchemas.init()
    mimir.algebra.spark.OperatorTranslation(this)
  }

  /**
   * Retrieve the query corresponding to the Lens or Virtual View with the specified
   * name (or None if no such lens exists)
   */
  def getView(name: String): Option[(Operator)] =
    getView(resolveCaseInsensitiveTable(name))
  def getView(name: ID): Option[(Operator)] =
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
  def fileToTableName(file: String): ID =
    ID(new File(file).getName.replaceAll("\\..*", ""))
  
  private val defaultLoadCSVOptions = Map(
    "ignoreLeadingWhiteSpace"-> "true",
    "ignoreTrailingWhiteSpace"-> "true", 
    "mode" -> "DROPMALFORMED", 
    "header" -> "false"
  )
  private val CSV = ID("csv")
  private val ErrorAwareCSV = ID("org.apache.spark.sql.execution.datasources.ubodin.csv")
  private val defaultLoadOptions = Map[ID, Map[String,String]](
    CSV           -> defaultLoadCSVOptions,
    ErrorAwareCSV -> defaultLoadCSVOptions
  )

  def loadTable(
    sourceFile: String, 
    targetTable: Option[ID] = None, 
    force:Boolean = false, 
    targetSchema: Option[Seq[(ID, Type)]] = None,
    inferTypes: Option[Boolean] = None,
    detectHeaders: Option[Boolean] = None,
    format: ID = ID("csv"),
    loadOptions: Map[String, String] = Map()
  ){
    // Pick a sane table name if necessary
    val realTargetTable = targetTable.getOrElse(fileToTableName(sourceFile))

    // If the backend is configured to support it, specialize data loading to support data warnings
    val datasourceErrors = loadOptions.getOrElse("datasourceErrors", "false").equals("true")
    val realFormat:ID = 
      if(datasourceErrors && format.equals(CSV)) {
        ErrorAwareCSV
      } else { format }

    val options = defaultLoadOptions.getOrElse(realFormat, Map()) ++ loadOptions

    val targetRaw = realTargetTable.withSuffix("_RAW")
    if(tableExists(targetRaw) && !force){
      throw new SQLException(s"Target table $realTargetTable already exists; Use `LOAD 'file' INTO tableName`; to append to existing data.")
    }
    if(!tableExists(realTargetTable)){
      LoadData.handleLoadTableRaw(
        this, 
        targetTable = targetRaw, 
        sourceFile = sourceFile,
        targetSchema = targetSchema, 
        options = options, 
        format = realFormat
      )
      var oper = table(targetRaw)
      //detect headers 
      if(datasourceErrors) {
        val dseSchemaName = realTargetTable.withSuffix("_DSE")
        adaptiveSchemas.create(dseSchemaName, ID("DATASOURCE_ERRORS"), oper, Seq())
        oper = adaptiveSchemas.viewFor(dseSchemaName, ID("DATA")).get
      }
      if(detectHeaders.getOrElse(targetSchema.isEmpty)) {
        val dhSchemaName = realTargetTable.withSuffix("_DH")
        adaptiveSchemas.create(dhSchemaName, ID("DETECT_HEADER"), oper, Seq())
        oper = adaptiveSchemas.viewFor(dhSchemaName, ID("DATA")).get
      }
      //type inference
      if(inferTypes.getOrElse(true)){
        val tiSchemaName = realTargetTable.withSuffix("_TI")
        adaptiveSchemas.create(tiSchemaName, ID("TYPE_INFERENCE"), oper, Seq(FloatPrimitive(.5))) 
        oper = adaptiveSchemas.viewFor(tiSchemaName, ID("DATA")).get
      }
      //finally create a view for the data
      views.create(realTargetTable, oper)
    } else {
      val schema = targetSchema match {
        case None => tableSchema(realTargetTable)
        case _ => targetSchema
      }
      LoadData.handleLoadTableRaw(
        this, 
        targetTable = realTargetTable, 
        sourceFile = sourceFile, 
        targetSchema = schema, 
        options = options, 
        format = realFormat
      )
    }
  }

  /**
    * Materialize a view into the database
    */
  def selectInto(targetTable: ID, sourceQuery: Operator){
    backend.createTable(targetTable, sourceQuery)
  }

  /**
   * Utility for modules to ensure that a table with the specified schema exists.
   *
   * If the table doesn't exist, it will be created.
   * If the table does exist, non-existant columns will be created.
   * If the table does exist and a column has a different type, an error will be thrown.
   */
  def requireMetadataTable(name: ID, schema: Seq[(ID, Type)], primaryKey: Option[ID] = None)
  {
    val typeMap = schema.toMap
    metadataBackend.getTableSchema(name) match {
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
        metadataBackend.update(createCmd);
      }
      case Some(oldSch) => {
        val currentColumns = oldSch.map { _._1 }.toSet
        for(column <- (typeMap.keySet ++ currentColumns)){
          if(typeMap contains column){
            if(!(currentColumns contains column)){
              logger.debug("Need to add $column to $name(${typemap.keys.mkString(", ")})")
              metadataBackend.update(s"ALTER TABLE $name ADD COLUMN $column ${typeMap(column)}")
            }
          }
        }
      }

    }
  }
}
