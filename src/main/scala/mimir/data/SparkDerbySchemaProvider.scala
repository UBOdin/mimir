package mimir.data

import mimir.exec.spark.{MimirSpark}

class SparkDerbySchemaProvider(database: String)
  extends SchemaProvider
  with LazyLogging
{
  def getAllTables(): Seq[ID] = {
    MimirSpark.get.sparkSession.catalog.listTables().collect().map(table => ID(table.name))
  }
  def invalidateCache() = {
    if(sparkSql == null) throw new Exception("There is no spark context")
    MimirSpark.get.sparkSession.catalog.clearCache()
  }
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