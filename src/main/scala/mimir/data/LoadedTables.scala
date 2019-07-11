package mimir.data

import play.api.libs.json._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import mimir.Database
import mimir.algebra._
import mimir.metadata._
import mimir.exec.spark.{MimirSpark,RAToSpark,RowIndexPlan}

// https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#run-sql-on-files-directly

class LoadedTables(db: Database)
  extends SchemaProvider
  with LazyLogging
{
  val cache = scala.collection.mutable.Map[String, DataFrame]()
  var store: MetadataMap = null

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
    mimirOptions: Map[String, String]
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
             mimirOptions = Json.parse(tableDefinition._2(3).asString).as[Map[String, String]]
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
    url: String, 
    format: String, 
    tableName: ID, 
    sparkOptions: Map[String,String] = Map(), 
    mimirOptions: Map[String,String] = Map()
  ) {
    if(tableExists(tableName)){
      throw new RAException(s"Can't LOAD ${tableName} because it already exists.")
    }

    // Some data loaders expect non-intuitive inputs.  First, do a little pre-processing to
    // make the interface more friendly.
    val finalURL = 
      format match {

        // The Google Sheets loader expects to see only the last two path components of 
        // the sheet URL.  Rewrite full URLs if the user wants.
        case LoadedTables.GOOGLE_SHEETS => 
          url.split("/").reverse.take(2).reverse.mkString("/")
        
        // For everything else use the URL unchanged
        case _ => url
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
      url = finalURL,
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
        StringPrimitive(Json.stringify(Json.toJson(mimirOptions)))
      )
    )
  }

  def drop(tableName: ID)
  {
    cache.remove(tableName.id)
    store.rm(tableName)
  }

  def tableOperator(tableName: ID, alias: ID): Operator = 
    tableOperator(LoadedTables.SCHEMA, tableName, alias)


}

object LoadedTables {
  val CSV                    = "csv"
  val JSON                   = "json"
  val GOOGLE_SHEETS          = "com.github.potix2.spark.google.spreadsheets"
  val CSV_WITH_ERRORCHECKING = "org.apache.spark.sql.execution.datasources.ubodin.csv"

  val SCHEMA = ID("DATA")

  private val defaultLoadCSVOptions = Map(
    "ignoreLeadingWhiteSpace"-> "true",
    "ignoreTrailingWhiteSpace"-> "true", 
    "mode" -> "DROPMALFORMED", 
    "header" -> "false"
  )
  val defaultLoadOptions = Map[String, Map[String,String]](
    CSV                    -> defaultLoadCSVOptions,
    CSV_WITH_ERRORCHECKING -> defaultLoadCSVOptions
  )

}
