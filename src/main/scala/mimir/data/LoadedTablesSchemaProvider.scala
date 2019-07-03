package mimir.data

// https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#run-sql-on-files-directly

class LoadedTablesSchemaProvider(db: Database)
{


  
  def listTables: Iterable[ID]
  def tableSchema(table: ID): Option[Seq[(ID, Type)]]
  def implementation(table: ID): Option[TableImplementation]
}