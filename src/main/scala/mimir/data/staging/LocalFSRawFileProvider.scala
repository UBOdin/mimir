package mimir.data.staging

import java.net.URL
import java.io.{ File, InputStream, OutputStream, FileOutputStream }
import java.sql.SQLException
import scala.util.Random
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame
import mimir.algebra.ID

/**
 * RawFileProvider backed by the local filesystem.
 * @param basePath  The path to stage files to (defaults to the current working directory)
 */
class LocalFSRawFileProvider(
  basePath: File = new File(".")
) extends RawFileProvider
    with LazyLogging
{
  /** 
   * Randomly allocate a name relative to basePath (internal only)
   * @param extension   The file extension to use for the allocated name
   * @param nameHint    A hint that will be part of the name
   * @return            A guaranteed unique file with the specified extension.
   */
  private def makeName(extension: String, nameHint: Option[String]): File =
  {
    val rand = new Random().alphanumeric
    // Try 1000 times to create a randomly named file
    for(i <- 0 until 1000){
      val candidate = new File(basePath,
        nameHint match { 
          case Some(hint) => s"${hint.replaceAll("[^a-zA-Z0-9]", "")}-${rand.take(10).mkString}.${extension}"
          case None => s"${rand.take(20).mkString}.${extension}"
        }
      )
      // If the randomly named file doesn't exist, we're done.
      if(!candidate.exists()){ return candidate }
    }
    // Fail after 1000 attempts.
    throw new SQLException(s"Can't allocate name for $nameHint")
  }

  /**
   * Transfer an InputStream to an OutputStream
   * @param input     The InputStream to read from
   * @param output    The OutputStream to write to
   *
   * Obsoleted in Java 9 with InputStream.transferTo... but we're on 8 for now
   */
  private def transferBytes(input: InputStream, output: OutputStream): Unit =
  {
    val buffer = Array.ofDim[Byte](1024*1024) // 1MB buffer
    var bytesRead = input.read(buffer)
    while(bytesRead >= 0) { 
      output.write(buffer, 0, bytesRead)
      bytesRead = input.read(buffer)
    }
  }

  def stage(input: InputStream, fileExtension: String, nameHint: Option[String]): String = 
  {
    val file = makeName(fileExtension, nameHint)
    transferBytes(input, new FileOutputStream(file))
    return file.toString
  }
  def stage(url: URL, nameHint: Option[String]): String =
  {
    val pathComponents = url.getPath.split("/")
    val nameComponents = pathComponents.reverse.head.split(".")
    val extension = 
      if(nameComponents.size > 1) { nameComponents.reverse.head }
      else { "data" } // default to generic 'data' if there's no extension
    stage(url.openStream(), extension, nameHint)
  }
  def stage(input: DataFrame, format: ID, nameHint:Option[String]): String =
  {
    val targetFile = makeName(format.id, nameHint).toString
    input.write
         .format(format.id)
         .save(targetFile)
    return targetFile
  }
  def drop(local: String): Unit = 
  {
    new File(local).delete()
  }
}