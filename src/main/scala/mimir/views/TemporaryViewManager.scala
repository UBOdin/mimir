package mimir.views

import mimir.Database
import mimir.algebra._
import mimir.data.SchemaProvider

class TemporaryViewManager(db: Database)
  extends scala.collection.mutable.LinkedHashMap[ID, Operator]
  with SchemaProvider
{
  def listTables: Iterable[ID] = keys
  def tableSchema(table: ID): Option[Seq[(ID, Type)]] = 
    get(table).map { db.typechecker.schemaOf(_) }
  def logicalplan(table: ID) = None
  def view(table: ID) = Some(get(table).get)
}

object TemporaryViewManager
{
  val SCHEMA = ID("TEMPORARY")
}