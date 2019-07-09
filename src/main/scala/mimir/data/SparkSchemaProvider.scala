package mimir.data

import mimir.metadata.SchemaProvider
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation

import mimir.Database
import mimir.algebra._
import mimir.exec.spark.{MimirSpark, RAToSpark, RowIndexPlan}

class SparkSchemaProvider(sparkDBName: String, db: Database)
  extends SchemaProvider
  with LazyLogging
{
  def listTables(): Seq[ID] = 
  {
    MimirSpark.get.sparkSession
              .catalog
              .listTables(sparkDBName)
              .collect()
              .map { table => ID(table.name) }
  }
  def invalidateCache() = {
    MimirSpark.get.sparkSession.catalog.clearCache()
  }

  def tableSchema(table: ID): Option[Seq[(ID, Type)]] = 
  {
    val catalog = MimirSpark.get.sparkSession.catalog

    if(catalog.tableExists(sparkDBName, table.id)){
      Some(catalog
              .listColumns(sparkDBName, table.id)
              .collect()
              .map { col => (
                  ID(col.name), 
                  RAToSpark.getMimirType( 
                    RAToSpark.dataTypeFromHiveDataTypeString(col.dataType))
                ) }
      )
    } else { None }
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
}


