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

/**
* @author ${user.name}
*/
object HadoopUtils {

  def writeToHDFS(sparkCtx:SparkContext, hdfsTargetFile:String, localFile:File) {
    val fs = FileSystem.get(sparkCtx.hadoopConfiguration)
    //fs.copyFromLocalFile(false, new Path(localFile.toURI()), new Path(hdfsTargetFile))
    val output = fs.create(new Path(hdfsTargetFile))
    val writer = new BufferedOutputStream(output)
    try {
        writer.write(Files.readAllBytes(Paths.get(localFile.getAbsolutePath))) 
    }
    finally {
        writer.close()
    }
  }

}