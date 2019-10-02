package mimir.data

import java.io.File
import java.sql.SQLException
import play.api.libs.json._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame

import mimir.Database
import mimir.algebra._
import mimir.metadata._
import mimir.exec.spark.{MimirSpark,RAToSpark}
import mimir.ctables.CoarseDependency

// https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#run-sql-on-files-directly

/**
 * Lazy data ingest for Mimir
 * 
 * Spark provides a Hive/Derby data storage framework that
 * you can use to ingest and locally store data.  However, 
 * using this feature has several major drawbacks:
 *   - It pollutes the local working directory unless you're
 *     connected to a HDFS cluster or have S3 set up.
 *     https://github.com/UBOdin/mimir/issues/321
 *   - It forces all data access to go through Spark, even
 *     if it might be more efficient to do so locally
 *     https://github.com/UBOdin/mimir/issues/322
 *   - It makes processing multi-table files / URLs much
 *     harder by forcing all access to go through the
 *     Dataset API.  
 *   - It makes provenance tracking harder, since we lose
 *     track of exactly where tables came from, as well as
 *     which tables are ingested/managed by Mimir.  We also
 *     lose the original data / URL.
 *    
 * This class allows Mimir to provide lower-overhead 
 * data source management than Derby. Schema information
 * is stored in Mimir's metadata store, and LoadedTables
 * dynamically creates DataFrames whenever the table is 
 * queried.
 */
class LoadedTables(val db: Database)
  extends DataFrameSchemaProvider
  with MaterializedTableProvider
  with LazyLogging
{
  val cache = scala.collection.mutable.Map[String, DataFrame]()
  var store: MetadataMap = null
  var bulkStorageDirectory = new File(".")
  var bulkStorageFormat = FileFormat.PARQUET

  /** 
   * Prepare LoadedTables for use with a given Mimir database.
   * 
   * This function registers LoadedTables metadata requirements
   * with a Mimir instance.
   */
  def init()
  {
    store = db.metadata.registerMap(
      ID("MIMIR_LOADED_TABLES"), 
      Seq(
        InitMap(Seq(
          ID("URL") -> TString(),
          ID("FORMAT") -> TString(),
          ID("SPARK_OPTIONS") -> TString(),
          ID("MIMIR_OPTIONS") -> TString()
        ))
    ))
  }

  def makeDataFrame(
    url: String, 
    format: ID, 
    sparkOptions: Map[String, String]
  ): DataFrame = 
  {
    var parser = MimirSpark.get.sparkSession.read.format(format.id)
    for((option, value) <- sparkOptions){
      parser = parser.option(option, value)
    }
    logger.trace(s"Creating dataframe for $format file from $url")
    // parser = parser.schema(RAToSpark.mimirSchemaToStructType(customSchema))
    return parser.load(url)
  }

  def loadDataframe(table: ID): Option[DataFrame] =
  {
    logger.trace(s"Loading $table")
         // If we have a record of the table (Some(tableDefinition))
    store.get(table)
         // Then load the dataframe normally
         .map { tableDefinition => 
           makeDataFrame(
             url = tableDefinition._2(0).asString,
             format = ID(tableDefinition._2(1).asString),
             sparkOptions = Json.parse(tableDefinition._2(2).asString).as[Map[String, String]]
           )
         }
         // and persist it in cache
         .map { df => cache.put(table.id, df); df }
         // otherwise fall through return None
  }

  /**
   * Recover the dataframe for a given loaded table
   * 
   * @param table  The table to load
   */
  def dataframe(table: ID): DataFrame =
  {
    cache.get(table.id)
         .getOrElse { loadDataframe(table).get }
  }

  def listTables: Iterable[ID] = 
    store.keys

  def tableSchema(table: ID): Option[Seq[(ID, Type)]] =
  {
    var dfOption = cache.get(table.id)
    if(dfOption == None){ dfOption = loadDataframe(table) }

    dfOption.map { _.schema }
            .map { RAToSpark.structTypeToMimirSchema(_) }
  }

  def stage(
    url: String, 
    sparkOptions: Map[String,String], 
    format: ID, 
    tableName: ID
  ): (String, Map[String,String], ID) =
  {
    if(LoadedTables.safeForRawStaging(format)){
      ( 
        db.staging.stage(url, Some(tableName.id)),
        sparkOptions,
        format
      )
    } else {
      (
        db.staging.stage(
          makeDataFrame(url, format, sparkOptions),
          bulkStorageFormat,
          Some(tableName.id)
        ),
        Map(),
        bulkStorageFormat
      )
    }
  }

  /**
   * Connect the specified URL to Mimir as a table.
   *
   * Note: This function does NOT stage data into a storage backend.
   * 
   * @param url           The URL to connect
   * @param format        The data loader to use
   * @param tableName     The name to give the URL
   * @param sparkOptions   Optional: Any pass-through parameters to provide the spark dataloader
   * @param mimirOptions   Optional: Any data loading parameters for Mimir itself (currently unused)
   */
  def linkTable(
    sourceFile: String, 
    format: ID, 
    targetTable: ID, 
    sparkOptions: Map[String,String] = Map(), 
    stageSourceURL: Boolean = false
  ) {
    if(tableExists(targetTable)){
      throw new RAException(s"Can't LOAD ${targetTable} because it already exists.")
    }
    // Some parameters may change during the loading process.  Var-ify them
    var url = sourceFile
    var storageFormat = format
    var finalSparkOptions = 
      LoadedTables.defaultLoadOptions
                  .getOrElse(format, Map()) ++ sparkOptions

    // Build a preliminary configuration of Mimir-specific metadata
    val mimirOptions = scala.collection.mutable.Map[String, JsValue]()

    val stagingIsMandatory = (
         sourceFile.startsWith("http://")
      || sourceFile.startsWith("https://")
    )
    // Do some pre-processing / default configuration for specific formats
    //  to make the API a little friendlier.
    storageFormat match {

      // The Google Sheets loader expects to see only the last two path components of 
      // the sheet URL.  Rewrite full URLs if the user wants.
      case FileFormat.GOOGLE_SHEETS => {
        url = url.split("/").reverse.take(2).reverse.mkString("/")
      }
      
      // For everything else do nothing
      case _ => {}
    }

    if(stageSourceURL || stagingIsMandatory) {
      // Preserve the original URL and configurations in the mimirOptions
      mimirOptions("preStagedUrl") = JsString(url)
      mimirOptions("preStagedSparkOptions") = Json.toJson(finalSparkOptions)
      mimirOptions("preStagedFormat") = JsString(storageFormat.id)
      val stagedConfig  = stage(url, finalSparkOptions, storageFormat, targetTable)
      url               = stagedConfig._1
      finalSparkOptions = stagedConfig._2
      storageFormat     = stagedConfig._3
    }

    // Not sure when exactly cache invalidation is needed within Spark.  It was in the old 
    // data loader, but it would only get called when the URL needed to be staged.  Since 
    // we're not staging here, might be safe to skip?  TODO: Ask Mike.
    // 
    // MimirSpark.get.sparkSession.sharedState.cacheManager.recacheByPath(
    //   MimirSpark.get.sparkSession, 
    //   url
    // )

    // Try to create the dataframe to make sure everything (parameters, etc...) are ok
    val df = makeDataFrame(
      url = url,
      format = storageFormat,
      sparkOptions = finalSparkOptions
    )
    // Cache the result
    cache.put(targetTable.id, df)

    // Save the parameters
    store.put(
      targetTable, Seq(
        StringPrimitive(url),
        StringPrimitive(format.id),
        StringPrimitive(Json.stringify(Json.toJson(sparkOptions))),
        StringPrimitive(Json.stringify(new JsObject(mimirOptions)))
      )
    )
  }

  def drop(tableName: ID, ifExists: Boolean = false)
  {
    store.get(tableName) match {
      case None if ifExists => {}
      case None => throw new SQLException(s"Loaded table $tableName does not exist")
      case Some((_, config)) => {
        val cascadingDrops = parseMimirOptions(config)
                                .getOrElse("cascadingDrops", { new JsArray(Seq[JsValue]().toIndexedSeq) })
                                .as[Seq[Map[String, String]]]
        for(objectToDrop <- cascadingDrops) {
          objectToDrop("type") match {
            case "adaptive" => db.adaptiveSchemas.drop(ID(objectToDrop("id")))
            case "view" => db.views.drop(ID(objectToDrop("id")))
            case "lens" => db.lenses.drop(ID(objectToDrop("id")))
          }
        }
        cache.remove(tableName.id)
        store.rm(tableName)
      }
    }
  }

  private def parseMimirOptions(config: Seq[PrimitiveValue]): Map[String, JsValue] =
    Json.parse(config(3).asString).as[Map[String, JsValue]]

  private def updateMimirOptions(tableName: ID)(update: (Map[String, JsValue] => Map[String, JsValue])): Unit =
  {
    store.get(tableName) match {
      case None => throw new SQLException(s"Loaded table $tableName does not exist")
      case Some((_, config)) => {
        store.update(tableName, Map(
          ID("MIMIR_OPTIONS") -> 
            StringPrimitive(
              Json.stringify(
                Json.toJson(
                  update(parseMimirOptions(config)))))
        ))
      }
    }
  }

  private def cascadeDrop(tableName: ID, viewName: ID, t: String): Unit =
    updateMimirOptions(tableName){ 
      oldOptions => 
        oldOptions ++ Map("cascadingDrops" -> 
          Json.toJson(
            oldOptions.get("cascadingDrops")
              .map { _.as[Seq[Map[String,String]]] }
              .getOrElse { Seq() } ++
              Seq( Map("type" -> t, "id" -> viewName.id) )
          )
        ):Map[String, JsValue]
    }
  def cascadeDropView(tableName: ID, viewName: ID): Unit = 
    cascadeDrop(tableName, viewName, "view")
  def cascadeDropLens(tableName: ID, viewName: ID): Unit = 
    cascadeDrop(tableName, viewName, "lens")
  def cascadeDropAdaptive(tableName: ID, adaptiveName: ID): Unit = 
    cascadeDrop(tableName, adaptiveName , "adaptive")

  def tableOperator(tableName: ID): Operator = 
    tableOperator(LoadedTables.SCHEMA, tableName)

  def createStoredTableAs(data: DataFrame, tableName: ID)
  {
    if(tableExists(tableName)){ 
      throw new SQLException(s"Table `$tableName` already exists.")
    }

    val targetFile = db.staging.stage(data, bulkStorageFormat, Some(tableName.id))

    linkTable(
      targetFile, 
      bulkStorageFormat,
      tableName
    )
  }

  def deleteStoredTableFiles(target: File, tableName: String):Unit =
  {
    if(target.isDirectory){
      target.listFiles.foreach { deleteStoredTableFiles(_, tableName) }
    }
    if(!target.delete()){
      throw new SQLException(s"Could not delete stored file for $tableName ($target)")
    }
  }

  def dropStoredTable(tableName: ID)
  {
    val file = 
      store.get(tableName)
           .getOrElse { throw new SQLException(s"Table `$tableName` does not exist.") }
           ._2(0) // get the 0th field of the data record (url)
           .asString
    drop(tableName)
    deleteStoredTableFiles(new File(file), tableName.id)
  }

  def fileToTableName(file: String): ID =
    ID(
      new File(file)
        .getName
        .replaceAll("\\s", "")             // strip out whitespace
        .replaceAll("\\..*", "")           // strip off the file extension
        .replaceAll("[^a-zA-Z0-9]+", "_")  // replace nonalphanums with '_'
        .replaceFirst("^[0-9_]+", "")      // strip off leading numerics
        .toUpperCase                       // Upcase
          match {                          // make sure that the name is not empty
            case "" => "MY_DATA"
            case something => something
          }
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
    sourceFile: String, 
    targetTable: Option[ID] = None,
    // targetSchema: Option[Seq[(ID, Type)]] = None,
    inferTypes: Option[Boolean] = None,
    detectHeaders: Option[Boolean] = None,
    format: ID = FileFormat.CSV,
    sparkOptions: Map[String, String] = Map(),
    humanReadableName: Option[String] = None,
    datasourceErrors: Boolean = true,
    stageSourceURL: Boolean = false,
    targetSchema: Option[Seq[ID]] = None
  ){
    // Pick a sane table name if necessary
    val realTargetTable = targetTable.getOrElse(fileToTableName(sourceFile))

    // If the backend is configured to support it, specialize data loading to support data warnings
    val realFormat = 
      format match {
        case FileFormat.CSV if datasourceErrors => 
          FileFormat.CSV_WITH_ERRORCHECKING
        case _ => format
      }

    val targetRaw = realTargetTable//.withSuffix("_RAW")
    if(db.catalog.tableExists(targetRaw)){
      throw new SQLException(s"Target table $realTargetTable already exists")
    }
    logger.trace("LOAD TABLE $realTargetTable <- $format($sourceFile);")
    linkTable(
      sourceFile = sourceFile,
      format = realFormat,
      targetTable = targetRaw,
      sparkOptions = sparkOptions,
      stageSourceURL = stageSourceURL
    )
    var oper = tableOperator(targetRaw)
    var needView = false
    logger.trace("Operator: $oper")
    //detect headers 
    if(LoadedTables.usesDatasourceErrors(realFormat)) {
      logger.trace(s"LOAD TABLE $realTargetTable: Adding datasourceErrors")
      val dseSchemaName = realTargetTable.withSuffix("_DSE")
      db.adaptiveSchemas.create(dseSchemaName, ID("DATASOURCE_ERRORS"), oper, Seq(), humanReadableName.getOrElse(realTargetTable.id))
      oper = db.adaptiveSchemas.viewFor(dseSchemaName, ID("DATA")).get
      cascadeDropAdaptive(targetRaw, dseSchemaName)
      needView = true
    }
    if(detectHeaders.getOrElse(true)) {
      logger.trace(s"LOAD TABLE $realTargetTable: Adding detectHeaders")
      val dhSchemaName = realTargetTable.withSuffix("_DH")
      db.adaptiveSchemas.create(dhSchemaName, ID("DETECT_HEADER"), oper, Seq(), humanReadableName.getOrElse(realTargetTable.id))
      oper = db.adaptiveSchemas.viewFor(dhSchemaName, ID("DATA")).get
      cascadeDropAdaptive(targetRaw, dhSchemaName)
      needView = true
    }
    //type inference
    if(inferTypes.getOrElse(true)){
      logger.trace(s"LOAD TABLE $realTargetTable: Adding inferTypes")
      val tiSchemaName = realTargetTable.withSuffix("_TI")
      db.adaptiveSchemas.create(tiSchemaName, ID("TYPE_INFERENCE"), oper, Seq(FloatPrimitive(.5)), humanReadableName.getOrElse(realTargetTable.id)) 
      oper = db.adaptiveSchemas.viewFor(tiSchemaName, ID("DATA")).get
      cascadeDropAdaptive(targetRaw, tiSchemaName)
      needView = true
    }
    for(schema <- targetSchema){
      logger.trace(s"LOAD TABLE $realTargetTable: Wrapping with target schema")
      oper = oper.renameByID(
        oper.columnNames.zip(schema):_*
      )
      needView = true
    }
    if(needView){
      //finally create a view for the data
      logger.trace(s"LOAD TABLE $realTargetTable: Creating wrapper view")
      db.views.create(realTargetTable, oper, force = true)
      cascadeDropView(targetRaw, realTargetTable)
    }
    logger.trace(s"LOAD TABLE $realTargetTable: Done!")

  }

  def reloadTable(table:ID): Unit =
  {
    val config = store.get(table)
                      .getOrElse { throw new SQLException(s"Undefined table $table") }
    val mimirOptions = parseMimirOptions(config._2)

    (
      mimirOptions.get("preStagedUrl"),
      mimirOptions.get("preStagedSparkOptions"),
      mimirOptions.get("preStagedFormat")
    ) match { 
      case (Some(url), Some(sparkOptions), Some(format)) => {
        val stagedConfig  = stage(
          url.as[String], 
          sparkOptions.as[Map[String,String]], 
          ID(format.as[String]),
          table
        )
        store.put((table, 
          Seq(
            StringPrimitive(stagedConfig._1), // url
            StringPrimitive(stagedConfig._3.id),
            StringPrimitive(Json.stringify(Json.toJson(stagedConfig._2))), // sparkOptions
            StringPrimitive(Json.stringify(new JsObject(mimirOptions)))
          )
        ))
      }
      case _ => {} // not a staged table, don't need to refresh
    }

    MimirSpark.get.sparkSession.sharedState.cacheManager.recacheByPath(
       MimirSpark.get.sparkSession, 
       config._2(0).asString
    )
  }

}

object LoadedTables {
  val SCHEMA = ID("IMPORTED")
  
  val safeForRawStaging = Set(
    FileFormat.CSV ,
    FileFormat.CSV_WITH_ERRORCHECKING,
    FileFormat.JSON,
    FileFormat.EXCEL,
    FileFormat.XML,
    FileFormat.TEXT
  )

  private val defaultLoadCSVOptions = Map(
    "ignoreLeadingWhiteSpace"-> "true",
    "ignoreTrailingWhiteSpace"-> "true", 
    "mode" -> "DROPMALFORMED", 
    "header" -> "false"
  )
  val defaultLoadOptions = Map[ID, Map[String,String]](
    FileFormat.CSV                    -> defaultLoadCSVOptions,
    FileFormat.CSV_WITH_ERRORCHECKING -> defaultLoadCSVOptions
  )

  val usesDatasourceErrors = Set(
    FileFormat.CSV_WITH_ERRORCHECKING
  )

}
