package mimir.data

import mimir.algebra.ID

object FileFormat {
  type T = ID

  val CSV                    = ID("csv")
  val JSON                   = ID("json")
  val XML                    = ID("com.databricks.spark.xml")
  val EXCEL                  = ID("com.crealytics.spark.excel")
  val JDBC                   = ID("jdbc")
  val TEXT                   = ID("text")
  val PARQUET                = ID("parquet")
  val PDF                    = ID("mimir.exec.spark.datasource.pdf")
  val ORC                    = ID("orc")
  val GOOGLE_SHEETS          = ID("mimir.exec.spark.datasource.google.spreadsheet")
  val CSV_WITH_ERRORCHECKING = ID("org.apache.spark.sql.execution.datasources.ubodin.csv")
}
