package mimir.data

import java.io.File
import java.sql.SQLException
import play.api.libs.json._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import mimir.Database
import mimir.algebra._
import mimir.metadata._
import mimir.exec.spark.{MimirSpark,RAToSpark,RowIndexPlan}

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
class LoadedTables(db: Database)
  extends SchemaProvider
  with BulkStorageProvider
  with LazyLogging
{
  val cache = scala.collection.mutable.Map[String, DataFrame]()
  var store: MetadataMap = null
  var bulkStorageDirectory = new File(".")
  var bulkStorageFormat = LoadedTables.Format.PARQUET

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
    format: String, 
    sparkOptions: Map[String, String], 
    mimirOptions: JsObject
  ): DataFrame = 
  {
    var parser = MimirSpark.get.sparkSession.read.format(format)
    for((option, value) <- sparkOptions){
      parser = parser.option(option, value)
    }
    // parser = parser.schema(RAToSpark.mimirSchemaToStructType(customSchema))
    return parser.load(url)
  }

  def loadDataframe(table: ID): Option[DataFrame] =
  {
         // If we have a record of the table (Some(tableDefinition))
    store.get(table)
         // Then load the dataframe normally
         .map { tableDefinition => 
           makeDataFrame(
             url = tableDefinition._2(0).asString,
             format = tableDefinition._2(1).asString,
             sparkOptions = Json.parse(tableDefinition._2(2).asString).as[Map[String, String]],
             mimirOptions = Json.parse(tableDefinition._2(3).asString).as[JsObject]
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

  def logicalplan(table: ID): Option[LogicalPlan] = 
    Some(
      RowIndexPlan(
        dataframe(table).queryExecution.logical,
        tableSchema(table).get
      ).getPlan(db)
    )
  
  def view(table: ID): Option[Operator] = None

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
    source: String, 
    format: String, 
    tableName: ID, 
    sparkOptions: Map[String,String] = Map(), 
    mimirOptions: JsObject = new JsObject(Map())
  ) {
    if(tableExists(tableName)){
      throw new RAException(s"Can't LOAD ${tableName} because it already exists.")
    }

    // Some data loaders expect non-intuitive inputs.  First, do a little pre-processing to
    // make the interface more friendly.
    val url = 
      format match {

        // The Google Sheets loader expects to see only the last two path components of 
        // the sheet URL.  Rewrite full URLs if the user wants.
        case LoadedTables.Format.GOOGLE_SHEETS => 
          source.split("/").reverse.take(2).reverse.mkString("/")
        
        // For everything else use the URL unchanged
        case _ => source
      }

    // Set required defaults
    val finalSparkOptions = 
      LoadedTables.defaultLoadOptions
                  .getOrElse(format, Map()) ++ sparkOptions

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
      format = format,
      sparkOptions = finalSparkOptions,
      mimirOptions = mimirOptions
    )
    // Cache the result
    cache.put(tableName.id, df)

    // Save the parameters
    store.put(
      tableName, Seq(
        StringPrimitive(url),
        StringPrimitive(format),
        StringPrimitive(Json.stringify(Json.toJson(sparkOptions))),
        StringPrimitive(Json.stringify(mimirOptions))
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
                                .getOrElse("cascadingDrops", { new JsArray(Seq()) })
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
    val targetFile = new File(bulkStorageDirectory, s"$tableName.$bulkStorageFormat").toString
    data.write
        .format(bulkStorageFormat)
        .save(targetFile)
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

}

object LoadedTables {
  object Format {
    val CSV                    = "csv"
    val JSON                   = "json"
    val XML                    = "com.databricks.spark.xml"
    val EXCEL                  = "com.crealytics.spark.excel"
    val JDBC                   = "jdbc"
    val TEXT                   = "text"
    val PARQUET                = "parquet"
    val ORC                    = "orc"
    val GOOGLE_SHEETS          = "com.github.potix2.spark.google.spreadsheets"
    val CSV_WITH_ERRORCHECKING = "org.apache.spark.sql.execution.datasources.ubodin.csv"
  }

  val SCHEMA = ID("IMPORTED")

  private val defaultLoadCSVOptions = Map(
    "ignoreLeadingWhiteSpace"-> "true",
    "ignoreTrailingWhiteSpace"-> "true", 
    "mode" -> "DROPMALFORMED", 
    "header" -> "false"
  )
  val defaultLoadOptions = Map[String, Map[String,String]](
    Format.CSV                    -> defaultLoadCSVOptions,
    Format.CSV_WITH_ERRORCHECKING -> defaultLoadCSVOptions
  )

}
