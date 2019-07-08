package mimir.views

class TransientViews(db: Database)
  extends scala.collection.mutable.Map[ID, Operator]
  with SchemaProvider
{
  def listTables: Iterable[ID] = keys
  def tableSchema(table: ID): Option[Seq[(ID, Type)]] = 
    get(table).map { db.typechecker.schemaOf(_) }
  def logicalplan(table: ID) = None
  def view(table: ID) = Some(get(table).get)
}