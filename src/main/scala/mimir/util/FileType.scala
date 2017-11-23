package mimir.util

import org.apache.commons.io.FilenameUtils

object FileType extends Enumeration
{
  type T = Value
  val SQLiteDB, 
      CSVFile, 
      TSVFile,
      TextFile,
      SQLFile = Value

  def detect(file: String): FileType.T =
  {
    FilenameUtils.getExtension(file).toLowerCase match {
      case "db" | "sqlite" => SQLiteDB
      case "sql"           => SQLFile
      case "csv"           => CSVFile
      case "tsv"           => TSVFile
      case _               => TextFile
    }
  }

}