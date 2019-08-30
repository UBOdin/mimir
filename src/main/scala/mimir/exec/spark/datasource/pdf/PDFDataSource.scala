package mimir.exec.spark.datasource.pdf


import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources.v2.reader._
import scala.collection.JavaConverters._
import org.apache.spark.sql.catalyst.InternalRow
import java.util.{Collections, List => JList, Optional}
import org.apache.spark.unsafe.types.UTF8String
import mimir.exec.spark.datasource.csv.CSVDataSourceReader

class DefaultSource extends DataSourceV2 with ReadSupport {

  def createReader(options: DataSourceOptions) = {
    val path = options.get("path").get
    val pages = options.get("pages").orElse("all")
    val hasGrid = options.get("gridLines").orElse("false").toBoolean
    val pdfExtractor = new PDFTableExtractor()
    val outPath = s"${path}.csv"
    pdfExtractor.defaultExtract(path, pages, Some(outPath), hasGrid)
    new CSVDataSourceReader(outPath, options.asMap().asScala.toMap)
  }
}



