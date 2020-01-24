package mimir.util

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.PrintWriter;
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.io.BufferedOutputStream
import org.apache.spark.SparkContext
import java.io.BufferedInputStream
import org.apache.hadoop.fs.permission.FsPermission
import java.io.InputStream

/**
* @author ${user.name}
*/
object HadoopUtils {

  def writeToHDFS(sparkCtx:SparkContext, hdfsTargetFile:String, input:InputStream, overwrite:Boolean ) {
    val fs = FileSystem.get(sparkCtx.hadoopConfiguration)
    //fs.copyFromLocalFile(false, new Path(localFile.toURI()), new Path(hdfsTargetFile))
    val hdfsPath = new Path(hdfsTargetFile)
    val exists = fs.exists(hdfsPath)
    if(exists && !overwrite){
      return
    }
    /*val output = if(!exists){
      fs.create(hdfsPath)
    }
    else {
      if(force){
        fs.delete(hdfsPath, true)
        fs.create(hdfsPath)
      }
      else throw new Exception("HDFS File already exists: " + hdfsTargetFile)
    }*/
    val output = fs.create(hdfsPath, overwrite)
    val writer = new BufferedOutputStream(output)
    try {
      val bytes = new Array[Byte](1024) //1024 bytes - Buffer size
      Iterator
      .continually (input.read(bytes))
      .takeWhile (_ != -1)
      .foreach (read=>writer.write(bytes,0,read))  
    }
    catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }
    finally {
         try {
           writer.close()
         }
         catch {
           case t: Throwable => t.printStackTrace() // TODO: handle error
         }
    }
  }
  
  def writeToHDFS(sparkCtx:SparkContext, hdfsTargetFile:String, localFile:File, overwrite:Boolean = false) {
    try {
      val srcUrl = if(localFile.getPath.contains(":/")) new java.net.URL(localFile.getPath.replaceFirst(":/", "://")) else localFile.toURI().toURL()
      val input = new BufferedInputStream(srcUrl.openStream)
      writeToHDFS(sparkCtx, hdfsTargetFile, input, overwrite)
    }
    catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }
  }
  
  def deleteFromHDFS(sparkCtx:SparkContext, hdfsTargetFile:String, recursive:Boolean = true) {
    val fs = FileSystem.get(sparkCtx.hadoopConfiguration)
    //fs.copyFromLocalFile(false, new Path(localFile.toURI()), new Path(hdfsTargetFile))
    val hdfsPath = new Path(hdfsTargetFile)
    val exists = fs.exists(hdfsPath)
    if(exists){
      fs.delete(hdfsPath, recursive)
    }
  }
  
  def fileExistsHDFS(sparkCtx:SparkContext, hdfsTargetFile:String) : Boolean = {
    val fs = FileSystem.get(sparkCtx.hadoopConfiguration)
    val hdfsPath = new Path(hdfsTargetFile)
    fs.exists(hdfsPath)
  }
  
  def getHomeDirectoryHDFS(sparkCtx:SparkContext) : String = {
    val fs = FileSystem.get(sparkCtx.hadoopConfiguration)
    fs.getHomeDirectory.toString()
  }
  
  def writeFilesInDirToHDFS(sparkCtx:SparkContext, destDirPath:String, srcDir:File, overwrite:Boolean = false) = {
    val fs = FileSystem.get(sparkCtx.hadoopConfiguration)
    val files: Array[File] = srcDir.listFiles()
    if (files != null && files.length > 0) {
      for (file <- files) {
        fs.copyFromLocalFile(false, overwrite, new Path(file.getPath()),
                new Path(destDirPath, file.getName()));
      }
    }
  }
  
  def writeDirToHDFS(sparkCtx:SparkContext, hdfsTargetFile:String, localFile:File, overwrite:Boolean = false) : Unit = {
    val fs = FileSystem.get(sparkCtx.hadoopConfiguration)
    val hdfsPath = new Path(hdfsTargetFile)
    fs.copyFromLocalFile(false, overwrite, new Path(localFile.getAbsolutePath), hdfsPath)
  }
  
  def setOwnerHDFS(sparkCtx:SparkContext, hdfsTargetFile:String, user:String, group:String) : Unit = {
    val fs = FileSystem.get(sparkCtx.hadoopConfiguration)
    val hdfsPath = new Path(hdfsTargetFile)
    fs.setOwner(hdfsPath, user, group)
  }
  
  def setPermissionsHDFS(sparkCtx:SparkContext, hdfsTargetFile:String, permission:Short) : Unit = {
    val fs = FileSystem.get(sparkCtx.hadoopConfiguration)
    val hdfsPath = new Path(hdfsTargetFile)
    fs.setPermission(hdfsPath, FsPermission.createImmutable(permission)) 
  }
  
  def readFromHDFS(sparkCtx:SparkContext, hdfsSrcFile:String, localFile:File) {
    val fs = FileSystem.get(sparkCtx.hadoopConfiguration)
    //fs.copyFromLocalFile(false, new Path(localFile.toURI()), new Path(hdfsTargetFile))
    val hdfsPath = new Path(hdfsSrcFile)
    val exists = fs.exists(hdfsPath)
    if(!exists){
      throw new Exception("file does not exist in hdfs: " + hdfsSrcFile )
    }
    
    fs.copyToLocalFile(hdfsPath, new Path(localFile.toURI()))
    
  }

}