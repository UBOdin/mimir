package mimir.data.staging

import java.io.InputStream
import org.apache.spark.sql.DataFrame

/**
 * Implementations of this trait provide the ability to "stage" files either
 * on the local filesystem or on a raw file storage provider like HDFS or 
 * S3.  Note that this is distinct from MaterializedTableProvider for two 
 * reasons.
 * 
 * 1. MaterializedTableProvider supports *only* DataFrames.  Data stored there 
 *    must be tabular.  
 * 2. More subtly, the expectation is that once a table is instantiated through 
 *    MaterializedTableProvider, the resulting table immediately becomes visible 
 *    to Mimir's System Catalog.  Thus, every MaterializedTableProvider should
 *    also be a SchemaProvider.  There is no such expectation for RawFileProvider.
 */
trait RawFileProvider { 

  /**
   * Stage a file by URL.
   * @param url       The URL to stage locally
   * @param nameHint  A small string to include in the staged URL (for debugging) 
   * @return          The local URL of the staged file
   */ 
  def stage(url: String, nameHint: Option[String]): String =
    stage(new java.net.URL(url), nameHint)
  /**
   * Stage a file by URL.
   * @param url       The URL to stage locally
   * @param nameHint  A small string to include in the staged URL (for debugging) 
   * @return          The local URL of the staged file
   */ 
  def stage(url: java.net.URL, nameHint: Option[String]): String
  /**
   * Stage a file directly from an InputStream.
   * @param input          An InputStream with the data to stage
   * @param fileExtension  The (short) extension to add to the file (e.g., csv)
   * @param nameHint       A small string to include in the staged URL (for debugging) 
   * @return               The local URL of the staged file
   */ 
  def stage(input: InputStream, fileExtension: String, nameHint: Option[String]): String
  /**
   * Stage a file by materializing a DataFrame
   * @param input      The DataFrame to stage
   * @param format     The format to stage the file into (e.g., parquet)
   * @param nameHint   A small string to include in the staged URL (for debugging) 
   * @return           The local URL of the staged file
   */ 
  def stage(input: DataFrame, format: String, nameHint: Option[String]): String

  /**
   * Delete a locally staged file
   * @param local      The local URL of the file (should have been returned by a 'stage' method)
   */
  def drop(local: String): Unit
}
