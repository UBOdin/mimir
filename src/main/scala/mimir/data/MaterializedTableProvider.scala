package mimir.data

import org.apache.spark.sql.DataFrame

import mimir.Database
import mimir.algebra.{ ID, Operator }

trait MaterializedTableProvider
{
  def createStoredTableAs(data: DataFrame, name: ID): Unit
  def createStoredTableAs(query: Operator, name: ID, db: Database): Unit = 
    createStoredTableAs(db.compiler.compileToSparkWithRewrites(query), name)
  def dropStoredTable(name: ID): Unit
}