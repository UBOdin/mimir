package mimir.util

object FileType extends Enumeration
{
  type T = Value
  val SQLiteDB, 
      CSVFile, 
      TextFile,
      SQLFile = Value

  def detect(file: String): FileType.T =
  {
    FilenameUtils.getExtension(file).toLowerCase match {
      case "db" | "sqlite" => SQLiteDB
      case "sql"           => SQLFile
      case "csv" | "tsv"   => CSVFile
      case _               => TextFile
  }

}