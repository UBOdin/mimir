package mimir.data

import play.api.libs.json._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import mimir.Database
import mimir.algebra._
import mimir.metadata._
import mimir.exec.spark.MimirSpark
import mimir.exec.spark.RAToSpark

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
    Some(dataframe(table).queryExecution.logical)
  
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

  def tableOperator(tableName: ID, alias: ID): Operator = 
    tableOperator(LoadedTables.SCHEMA_NAME, tableName, alias)


}

object LoadedTables {
  val CSV                    = "csv"
  val JSON                   = "json"
  val GOOGLE_SHEETS          = "com.github.potix2.spark.google.spreadsheets"
  val CSV_WITH_ERRORCHECKING = "org.apache.spark.sql.execution.datasources.ubodin.csv"

  val SCHEMA_NAME = ID("LOADED_TABLES")

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


// def readDataSource(name:String, format:String, options:Map[String, String], schema:Option[Seq[(ID, Type)]], load:Option[String]) = {
//   if(sparkSql == null) throw new Exception("There is no spark context")
//   def copyToS3(file:String): String = {
//     val accessKeyId = System.getenv("AWS_ACCESS_KEY_ID")
//     val secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY")
//     val endpoint = System.getenv("S3_ENDPOINT") match {
//       case null => None
//       case "" => None
//       case x => Some(x)
//     }
//     val s3client = S3Utils.authenticate(accessKeyId, secretAccessKey, "us-east-1", endpoint)
//     var relPath = file.replaceFirst("https?://", "").replace(new File("").getAbsolutePath + File.separator, "")
//     while(relPath.startsWith(File.separator))
//       relPath = relPath.replaceFirst(File.separator, "")
//     logger.debug(s"upload to s3: $file -> $relPath")
//     //this is slower but will work with URLs and local files
//     S3Utils.copyToS3("mimir-test-data", file, relPath, s3client, overwriteStagedFiles)
//     //this is faster but does not work with URLs - only local files
//     //S3Utils.uploadFile("mimir-test-data", file, relPath, s3client, overwriteStagedFiles)
//     relPath
//   }
//   var  pathIfCSV = "("
//   val dsFormat = sparkSql.read.format(format) 
//   val dsOptions = options.toSeq.foldLeft(dsFormat)( (ds, opt) => ds.option(opt._1, opt._2))
//   val dsSchema = schema match {
//     case None => dsOptions
//     case Some(customSchema) => dsOptions.schema(OperatorTranslation.mimirSchemaToStructType(customSchema))
//   }
//   val df = (load match {
//     case None => dsSchema.load
//     case Some(ldf) => {
//       if(remoteSpark){
//         val fileNameParts = ldf.split(File.separator)
//         val fileName = fileNameParts.last
//         if(ldf.startsWith("s3n:/") || ldf.startsWith("s3a:/")){
//           sparkSql.sparkSession.sharedState.cacheManager.recacheByPath(sparkSql.sparkSession, ldf)
//           dsSchema.load(ldf)
//         }
//         else{
//           if(dataStagingType.equalsIgnoreCase("s3")){
//             dsSchema.load("s3n://mimir-test-data/"+copyToS3(ldf))
//           }
//           else{
//             val hdfsHome = HadoopUtils.getHomeDirectoryHDFS(sparkSql.sparkSession.sparkContext)
//             val uniqueFile = fileNameParts(fileNameParts.length-2) +"_" + fileNameParts.last 
//             logger.debug("Copy File To HDFS: " +hdfsHome+File.separator+uniqueFile)
//             //if(!HadoopUtils.fileExistsHDFS(sparkSql.sparkSession.sparkContext, fileName))
//             HadoopUtils.writeToHDFS(sparkSql.sparkSession.sparkContext, uniqueFile, new File(ldf), overwriteStagedFiles)
//             logger.debug("... done\n")
//             pathIfCSV = s"""(path "$hdfsHome/$uniqueFile", """
//             dsSchema.load(s"$hdfsHome/$uniqueFile")
//           }
//         }
//       }
//       else {
//         if(format.equals("com.github.potix2.spark.google.spreadsheets")){
//           val gsldfparts = ldf.split("\\/") 
//           val gsldf = s"${gsldfparts(gsldfparts.length-2)}/${gsldfparts(gsldfparts.length-1)}"
//           sparkSql.sparkSession.sharedState.cacheManager.recacheByPath(sparkSql.sparkSession, gsldf)
//           dsSchema.load(gsldf)
//         }
//         else if(ldf.startsWith("s3n:/") || ldf.startsWith("s3a:/") || !dataStagingType.equalsIgnoreCase("s3")){
//           sparkSql.sparkSession.sharedState.cacheManager.recacheByPath(sparkSql.sparkSession, ldf)
//           dsSchema.load(ldf)
//         } 
//         else {
//           dsSchema.load("s3n://mimir-test-data/"+copyToS3(ldf))
//         }
//       }
      
//     }
//   })/*.toDF(df.columns.map(_.toUpperCase): _*)*/  //.persist().createOrReplaceTempView(name)
  
//   // attempt 1 ---------------------
//   //try using builtin save funcionality to create persistent table
//   df.persist().createOrReplaceTempView(name) 
//   df.write.mode(SaveMode.ErrorIfExists).saveAsTable(name) // but resulting table ends up being empty
 
  
//   // attempt 2 ---------------------
//   //try makting temp view then create a persistent view of it
//   /*df.persist().createOrReplaceTempView(name)                          
//   //sparkSql.sparkSession.table(name)                              
//   val tableIdentifier = try {
//     sparkSql.sparkSession.sessionState.sqlParser.parseTableIdentifier(name)
//   } catch {
//     case _: Exception => throw new RAException(s"Invalid view name: name")
//   }
//   CreateViewCommand(
//     name = tableIdentifier,
//     userSpecifiedColumns = Nil,
//     comment = None,
//     properties = Map.empty,
//     originalText = Option(s"CREATE VIEW $name AS SELECT * FROM $name"),
//     child = df.queryExecution.logical,
//     allowExisting = false,
//     replace = true,
//     viewType = PersistedView).run(sparkSql.sparkSession)
//   */
   
//   // attempt 3 ---------------------
//   //try creating the table from the csv file then creating a persistent view
//   /*val createSql = s"CREATE TABLE ${name}_tmp (${df.schema.fields.map(schee => s"${schee.name} ${schee.dataType.sql}").mkString(",")}) USING $format OPTIONS ${options.map(opt => s"""${opt._1} "${opt._2}" """).mkString(pathIfCSV, ",", ")")}"
//   sparkSql.sql(createSql)
//   val tableIdentifier = try {                                                                             
//     sparkSql.sparkSession.sessionState.sqlParser.parseTableIdentifier(name)     
//   } catch {                                                                                                                      
//     case _: Exception => throw new RAException(s"Invalid view name: name")                                                  
//   }                     
//   CreateViewCommand(                                                                        
//     name = tableIdentifier,                                 
//     userSpecifiedColumns = df.schema.fields.map(schee => (s"${schee.name}", None)),
//     comment = None,                                                             
//     properties = Map.empty,                                     
//     originalText = Option(s"CREATE VIEW $name AS SELECT * FROM ${name}_tmp"),                                
//     child = df.queryExecution.logical,                                                             
//     allowExisting = false,                                                             
//     replace = true,                                                   
//     viewType = PersistedView).run(sparkSql.sparkSession) 
//   */  
    
 
//   // attempt 4 ---------------------
//   //this is the workaround in the docs @ https://www.cloudera.com/documentation/enterprise/release-notes/topics/cdh_rn_spark_ki.html#ki_sparksql_dataframe_saveastable
//   /*df.persist().createOrReplaceTempView(s"${name}_tmp")                         
//   val createSql = s"CREATE TABLE ${name} (${df.schema.fields.map(schee => s"${schee.name} ${schee.dataType.sql}").mkString(",")}) STORED AS parquet"
//   sparkSql.sql(createSql)
//   sparkSql.sql(s"INSERT INTO TABLE $name SELECT * FROM ${name}_tmp")
//   //sparkSql.sparkSession.table(name).show() //this is empty - wtf 
//   */
// }