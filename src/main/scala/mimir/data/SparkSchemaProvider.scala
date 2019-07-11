package mimir.data

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.{ DataFrame, SaveMode } 
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.analysis.{ UnresolvedRelation, NoSuchDatabaseException }
import org.apache.spark.sql.execution.command.{ DropTableCommand, CreateDatabaseCommand }

import mimir.Database
import mimir.algebra._
import mimir.exec.spark.{MimirSpark, RAToSpark, RowIndexPlan}

class SparkSchemaProvider(db: Database)
  extends SchemaProvider
  with BulkStorageProvider
  with LazyLogging
{

  def listTables(): Seq[ID] = 
  {
    try {
      val tables = 
        MimirSpark.get.sparkSession
                  .catalog
                  .listTables(/*sparkDBName*/)
                  .collect()
                  .map { table => ID(table.name) }
      logger.trace(s"Spark tables: ${tables.mkString(",")}")
      return tables
    } catch {
      case _:NoSuchDatabaseException => {
        logger.warn("Couldn't find database!!! ($sparkDBName)")
        Seq()
      }
    }
  }
  def invalidateCache() = {
    MimirSpark.get.sparkSession.catalog.clearCache()
  }

  def tableSchema(table: ID): Option[Seq[(ID, Type)]] = 
  {
    try {
      val catalog = MimirSpark.get.sparkSession.catalog

      logger.trace(s"Getting table schema for $table")

      if(catalog.tableExists(/*sparkDBName,*/ table.id)){
        Some(catalog
                .listColumns(/*sparkDBName,*/ table.id)
                .collect()
                .map { col => (
                    ID(col.name), 
                    RAToSpark.getMimirType( 
                      RAToSpark.dataTypeFromHiveDataTypeString(col.dataType))
                  ) }
        )
      } else { 
        logger.trace(s"$table doesn't exist")
        None 
      }
    } catch {
      case _:NoSuchDatabaseException => {
        logger.warn("Couldn't find database!!! ($sparkDBName)")
        None
      }
    }
  }

  def logicalplan(table: ID): Option[LogicalPlan] =
  {
    tableSchema(table)
      .map { realSchema => 
        RowIndexPlan(
          UnresolvedRelation(TableIdentifier(table.id)), realSchema
        ).getPlan(db)
      }
  }
  def view(table: ID): Option[Operator] = None

  def createStoredTableAs(data: DataFrame, name: ID)
  {
    data.persist()
        .createOrReplaceTempView(name.id)
    data.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(name.id)
  }

  def dropStoredTable(name: ID)
  {
    DropTableCommand(
      TableIdentifier(name.id, None),//Option(sparkDBName)), 
      true, false, true
    ).run(MimirSpark.get.sparkSession)
  }
}


