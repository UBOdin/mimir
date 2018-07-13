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
  

object BackupUtils {
  def main(args: Array[String]) {
    val bakupParams = classOf[BackupConfig].getDeclaredFields.map("--"+_.getName).toSet
    val mimirParams = classOf[MimirConfig].getDeclaredFields.map("--"+_.getName).toSet
    
    val config = new BackupConfig(args.filterNot((mimirParams&~bakupParams).contains(_)))
    if(config.backup() && config.restore()) throw new Exception("CANNOT backup and restore at once")
    Mimir.conf = new MimirConfig(args.filterNot((bakupParams&~mimirParams).contains(_)));

    ExperimentalOptions.enable(Mimir.conf.experimental())
    val database = Mimir.conf.dbname().split("[\\\\/]").last.replaceAll("\\..*", "")
    val sback = new SparkBackend(database)
    sback.open()
   
    println("backing up data....")
    println(config.summary)
    println(Mimir.conf.summary)
    if(config.backup())
      doBackup(sback.getSparkContext().sparkSession.sparkContext, config.s3Url(), config.dataDir())
    else if(config.restore())
      doRestore(sback.getSparkContext().sparkSession.sparkContext, config.s3Url(), config.dataDir())
  }
  
  def doBackup(sparkCtx:SparkContext, s3Bucket:String, dataDir:String) = {
    tarDirToS3(s3Bucket,dataDir,"vizier-data.tar")
    val hdfsHome = HadoopUtils.getHomeDirectoryHDFS(sparkCtx)
    val sparkMetastoreFile = new File("metadtore_db")
    HadoopUtils.readFromHDFS(sparkCtx, s"${hdfsHome}/metastore_db", sparkMetastoreFile)
    tarDirToS3(s3Bucket,sparkMetastoreFile.getAbsolutePath,"metastore.tar")
  }
  
  def doRestore(sparkCtx:SparkContext, s3Bucket:String, dataDir:String) = {
    untarFromS3(s3Bucket, "vizier-data.tar", dataDir)
    val hdfsHome = HadoopUtils.getHomeDirectoryHDFS(sparkCtx)
    val sparkMetastoreFile = new File("metadtore_db")
    untarFromS3(s3Bucket, "metastore.tar", sparkMetastoreFile.getPath)
    HadoopUtils.writeToHDFS(sparkCtx, s"${hdfsHome}/metastore_db", sparkMetastoreFile, true)
  }
  
  
  def tarDirToS3(s3Bucket:String, srcDir:String, targetFile:String) = {
    import sys.process._
    //tar up vizier data
    val tarCmd = Seq("tar", "-c", srcDir)
    val stdoutStream = new ByteArrayOutputStream
    val errorLog = new StringBuilder()
    val exitCode = tarCmd #> stdoutStream !< ProcessLogger(s => (errorLog.append(s+"\n")))
    if (exitCode == 0) {
      val accessKeyId = System.getenv("AWS_ACCESS_KEY_ID")
      val secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY")
      val s3client = S3Utils.authenticate(accessKeyId, secretAccessKey, "us-east-1")
      S3Utils.copyToS3Stream(s3Bucket, new ByteArrayInputStream(stdoutStream.toByteArray()), targetFile, s3client, true) 
    }
    else{
      throw new Exception(s"Failed To tar Directory and send to S3: exitcode: $exitCode\nwith errors:${errorLog.toString()}")
    }
  }
  
  def untarFromS3(s3Bucket:String, srcFile:String, targetDir:String) = {
    import sys.process._
    //tar up vizier data
    val accessKeyId = System.getenv("AWS_ACCESS_KEY_ID")
    val secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY")
    val s3client = S3Utils.authenticate(accessKeyId, secretAccessKey, "us-east-1")
    val tarCmd = Seq("tar", "-xpvC", targetDir)
    val stdoutStream = new ByteArrayOutputStream
    val errorLog = new StringBuilder()
    val exitCode = tarCmd #< S3Utils.readFromS3(s3Bucket, srcFile, s3client) #> stdoutStream !< ProcessLogger(s => (errorLog.append(s+"\n")))
    if (exitCode == 0) {
      
    }
    else{
      throw new Exception(s"Failed To download and untar file from S3: exitcode: $exitCode\nwith errors:${errorLog.toString()}")
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
  val s3Url = opt[String]("backupTo", descr = "The s3 bucket url to backup to or restore from",
    default = Some("s3n://mimir-test-data/backup/"))
  val dataDir = opt[String]("dataDir", descr = "The path of the vizier/mimir data",
    default = Some("/usr/local/source/web-api/.vizierdb/"))
  val backup = toggle("backup", default = Some(false),
      descrYes = "backup data for mimir to s3",
      descrNo = "backup data for mimir to s3")
  val restore = toggle("restore", default = Some(false),
      descrYes = "restore data for mimir from s3",
      descrNo = "restore data for mimir from s3")
}