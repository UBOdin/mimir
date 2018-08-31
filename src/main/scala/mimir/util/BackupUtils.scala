package mimir.util

import java.io.ByteArrayOutputStream
import java.io.PrintWriter
import org.rogach.scallop.ScallopConf
import mimir.Mimir
import mimir.MimirConfig
import mimir.sql.SparkBackend
import java.io.ByteArrayInputStream
import org.apache.spark.SparkContext
import java.io.File
import org.apache.spark.sql.execution.command.SetDatabaseCommand
  

object BackupUtils {
  var sback:SparkBackend = null;
  def main(args: Array[String]) {
    val bakupParams = classOf[BackupConfig].getDeclaredFields.map("--"+_.getName).toSet
    val mimirParams = classOf[MimirConfig].getDeclaredFields.map("--"+_.getName).toSet 
    
    var addNext = false
    val backupArgs = args.foldLeft(Array[String]())((init, curr) => {
      if(addNext) {
        addNext = false
        init :+ curr
      }
      else if(curr.equals("-X") || init.mkString(" ").matches(".*-X[a-zA-Z0-9 ]*$")){
        init :+ curr
      }
      else if(!bakupParams.contains(curr)){
        init
      }
      else {
        if(classOf[BackupConfig].getDeclaredField(curr.replaceAll("^-+", "")).getGenericType.getTypeName.equals("org.rogach.scallop.ScallopOption<java.lang.String>"))
          addNext = true
        init :+ curr
      }
    })
    addNext = false
    val mimirArgs = args.foldLeft(Array[String]())((init, curr) => {
      if(addNext) {
        addNext = false
        init :+ curr
      }
      else if(curr.equals("-X") || init.mkString(" ").matches(".*-X[a-zA-Z0-9 ]*$")){
        init :+ curr
      }
      else if(!mimirParams.contains(curr)){
        init 
      }
      else {
        if(classOf[MimirConfig].getDeclaredField(curr.replaceAll("^-+", "")).getGenericType.getTypeName.equals("org.rogach.scallop.ScallopOption<java.lang.String>"))
          addNext = true
        init :+ curr
      }
    })
    
    val config = new BackupConfig(backupArgs)
    if(config.backup() && config.restore()) throw new Exception("CANNOT backup and restore at once")
    Mimir.conf = new MimirConfig(mimirArgs);

    ExperimentalOptions.enable(Mimir.conf.experimental())
    val database = Mimir.conf.dbname().split("[\\\\/]").last.replaceAll("\\..*", "")
    sback = new SparkBackend(database, true)
    sback.open()
   
    println(config.summary)
    println(Mimir.conf.summary)
    if(config.backup())
      doBackup(sback.getSparkContext().sparkSession.sparkContext, config.s3Bucket(), config.s3BackupDir(), config.dataDir())
    else if(config.restore())
      doRestore(sback.getSparkContext().sparkSession.sparkContext, config.s3Bucket(), config.s3BackupDir(), config.dataDir())
  }
  
  def doBackup(sparkCtx:SparkContext, s3Bucket:String, backupDir:String, dataDir:String) = {
    tarDirToS3(s3Bucket,dataDir,backupDir+"/vizier-data.tar")
    val hdfsHome = HadoopUtils.getHomeDirectoryHDFS(sparkCtx)
    /*val sparkMetastoreFile = new File("hmetastore_db")
    if(sparkMetastoreFile.exists())
      deleteFile(sparkMetastoreFile)
    val localMetastoreFile = new File(s"${sparkMetastoreFile.getAbsolutePath}/metastore_db")
    HadoopUtils.readFromHDFS(sparkCtx, s"${hdfsHome}/metastore_db", localMetastoreFile)
    tarDirToS3(s3Bucket,localMetastoreFile.getAbsolutePath,backupDir+"/hmetastore.tar")*/
    
    val derbyMetastoreFile = new File("metastore_db")
    tarDirToS3(s3Bucket,derbyMetastoreFile.getAbsolutePath,backupDir+"/metastore.tar")
    
    val sparkDataFile = new File("user_data")
    if(sparkDataFile.exists())
      deleteFile(sparkDataFile)
    HadoopUtils.readFromHDFS(sparkCtx, s"${hdfsHome}/", sparkDataFile)
    tarDirToS3(s3Bucket,sparkDataFile.getAbsolutePath,backupDir+"/userdata.tar")
    
    /*val sparkTablesFile = new File("tables")
    if(sparkTablesFile.exists())
      deleteFile(sparkTablesFile)
    exportSparkTables(sparkTablesFile.getAbsolutePath, s"${hdfsHome}/tables/")
    tarDirToS3(s3Bucket,sparkTablesFile.getAbsolutePath,backupDir+"/sparktables.tar")*/
  }
  
  def doRestore(sparkCtx:SparkContext, s3Bucket:String, backupDir:String, dataDir:String) = {
    untarFromS3(s3Bucket, backupDir+"/vizier-data.tar", dataDir)
    val hdfsHome = HadoopUtils.getHomeDirectoryHDFS(sparkCtx)
    /*val sparkMetastoreFile = new File("hmetastore_db")
    if(sparkMetastoreFile.exists())
      deleteFile(sparkMetastoreFile)
    sparkMetastoreFile.mkdir()
    val localMetastoreFile = new File(s"${sparkMetastoreFile.getAbsolutePath}/metastore_db")
    untarFromS3(s3Bucket, backupDir+"/hmetastore.tar", localMetastoreFile.getAbsolutePath)
    HadoopUtils.deleteFromHDFS(sparkCtx, s"${hdfsHome}/metastore_db", true)
    HadoopUtils.writeDirToHDFS(sparkCtx, s"${hdfsHome}/", localMetastoreFile, true)
    deleteFile(sparkMetastoreFile)*/
    
    val derbyMetastoreFile = new File("metastore_db")
    if(derbyMetastoreFile.exists())
      deleteFile(derbyMetastoreFile)
    untarFromS3(s3Bucket, backupDir+"/metastore.tar", derbyMetastoreFile.getAbsolutePath)
      
    val sparkDataFile = new File("user_data")
    if(sparkDataFile.exists())
      deleteFile(sparkDataFile)
    untarFromS3(s3Bucket, backupDir+"/userdata.tar", new File(".").getAbsolutePath)
    HadoopUtils.writeFilesInDirToHDFS(sparkCtx, s"${hdfsHome}/", sparkDataFile, true)
    //HadoopUtils.setPermissionsHDFS(sparkCtx, s"${hdfsHome}/", 0x309:Short)
    deleteFile(sparkDataFile)
    
    /*val sparkTablesFile = new File("tables")
    if(sparkTablesFile.exists())
      deleteFile(sparkTablesFile)
    untarFromS3(s3Bucket, backupDir+"/sparktables.tar", new File(".").getAbsolutePath)
    importSparkTables(sparkTablesFile.getAbsolutePath, s"${hdfsHome}/tables/")
    deleteFile(sparkTablesFile)*/
  }
  
  def exportSparkTables(localPath:String, hdfsExportPath:String) = {
    val dbs = sback.getSparkContext().sparkSession.catalog.listDatabases().collect()
    dbs.map(sdb => { 
      SetDatabaseCommand(sdb.name).run(sback.getSparkContext().sparkSession)
      val tables = sback.getSparkContext().sparkSession.catalog.listTables(sdb.name).collect()
      tables.map(table => {
        val hdfsFile = s"${hdfsExportPath}${table.name}"
        sback.getSparkContext().sql(s"export table ${table.name} to '$hdfsFile'") 
        HadoopUtils.readFromHDFS(sback.getSparkContext().sparkSession.sparkContext, hdfsFile, new File(s"$localPath/${table.name}"))
      })
    })
  }
  
  def importSparkTables(localPath:String, hdfsImportPath:String) = {
    val files: Array[File] = new File(localPath).listFiles()
    if (files != null && files.length > 0) {
      for (file <- files) {
        val hdfsFile = s"${hdfsImportPath}${file.getName()}"
        HadoopUtils.writeToHDFS(sback.getSparkContext().sparkSession.sparkContext, hdfsFile, file, true)  
        sback.getSparkContext().sql(s"import from '$hdfsFile'")
      }
    }
  }
  
  def tarDirToS3(s3Bucket:String, srcDir:String, targetFile:String) = {
    import sys.process._
    //tar up vizier data
    val parentDir = Option(new File(srcDir).getParent + File.separator).getOrElse("/")
    val folder = srcDir.replace(parentDir, "")
    val tarCmd = Seq("tar", "-c", "-C", parentDir, folder)
    val stdoutStream = new ByteArrayOutputStream
    val errorLog = new StringBuilder()
    val exitCode = tarCmd #> stdoutStream !< ProcessLogger(s => (errorLog.append(s+"\n")))
    if (exitCode == 0) {
      val accessKeyId = System.getenv("AWS_ACCESS_KEY_ID")
      val secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY")
      val endpoint = Option(System.getenv("S3_ENDPOINT"))
      val s3client = S3Utils.authenticate(accessKeyId, secretAccessKey, "us-east-1", endpoint)
      S3Utils.copyToS3Stream(s3Bucket, new ByteArrayInputStream(stdoutStream.toByteArray()), targetFile, s3client, true) 
    }
    else{
      throw new Exception(s"Failed To tar Directory and send to S3: exitcode: $exitCode\nwith errors:${errorLog.toString()}")
    }
  }
  
  def untarFromS3(s3Bucket:String, srcFile:String, targetDir:String) = {
    import sys.process._
    //tar up vizier data
    val parentDir = Option(new File(targetDir).getParent + File.separator).getOrElse("/")
    val accessKeyId = System.getenv("AWS_ACCESS_KEY_ID")
    val secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY")
    val endpoint = Option(System.getenv("S3_ENDPOINT"))
    val s3client = S3Utils.authenticate(accessKeyId, secretAccessKey, "us-east-1", endpoint)
    val tarCmd = Seq("tar", "-xpv", "-C", parentDir)
    val stdoutStream = new ByteArrayOutputStream()
    val errorLog = new StringBuilder()
    val exitCode = tarCmd #< S3Utils.readFromS3(s3Bucket, srcFile, s3client) #> stdoutStream !< ProcessLogger(s => (errorLog.append(s+"\n")))
    if (exitCode == 0) {
      
    }
    else{
      throw new Exception(s"Failed To download and untar file from S3: exitcode: $exitCode\nwith errors:${errorLog.toString()}")
    }
  }
  
  def deleteFile(dir: File): Unit = {
    if (dir.isDirectory) {
      val files: Array[File] = dir.listFiles()
      if (files != null && files.length > 0) {
        for (aFile <- files) {
          deleteFile(aFile)
        }
      }
      dir.delete()
    } else {
      dir.delete()
    }
  }
  
}

class BackupConfig(arguments: Seq[String]) extends ScallopConf(arguments)
{
  val experimental = opt[List[String]]("X", default = Some(List[String]()))
  val sparkHost = opt[String]("sparkHost", descr = "The IP or hostname of the spark master",
    default = Some("spark-master.local"))
  val sparkPort = opt[String]("sparkPort", descr = "The port of the spark master",
    default = Some("7077"))
  val s3Bucket = opt[String]("s3Bucket", descr = "The s3 bucket url to backup to or restore from",
    default = Some("mimir-test-data"))
  val s3BackupDir = opt[String]("s3BackupDir", descr = "The path in s3 to store backup",
    default = Some("backup"))
  val dataDir = opt[String]("dataDir", descr = "The path of the vizier/mimir data",
    default = Some("/usr/local/source/web-api/.vizierdb/"))
  val backup = toggle("backup", default = Some(false),
      descrYes = "backup data for mimir to s3",
      descrNo = "backup data for mimir to s3")
  val restore = toggle("restore", default = Some(false),
      descrYes = "restore data for mimir from s3",
      descrNo = "restore data for mimir from s3")
}