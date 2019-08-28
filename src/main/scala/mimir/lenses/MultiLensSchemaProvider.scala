package mimir.lenses

import play.api.libs.json._

import mimir.Database
import mimir.algebra.{ ID, Operator, Type, LensView }
import mimir.data.ViewSchemaProvider
import mimir.exec.mode.UnannotatedBestGuess

case class MultiLensSchemaProvider(
  lens: MultiLens,
  db: Database,
  name: ID,
  query: Operator,
  config: JsValue, 
  friendlyName: String
)
  extends ViewSchemaProvider
{
  def listTables: Seq[ID] = 
    db.query(
      lens.tableCatalog(
        db,
        name,
        query,
        config,
        friendlyName
      ).projectByID( ID("TABLE_NAME") ),
      UnannotatedBestGuess
    ) { result => 
      result.map { row => ID(row(0).asString) }
            .toIndexedSeq 
    }

  def tableSchema(table: ID): Option[Seq[(ID, Type)]] =
    lens.view(
      db, 
      name,
      table,
      query,
      config, 
      friendlyName
    ).map { db.typechecker.schemaOf(_) }

  def view(table: ID) =
    LensView(
      Some(name),
      table, 
      lens.view(
        db, 
        name,
        table,
        query,
        config, 
        friendlyName
      ).get
    )

  override def listTablesQuery: Operator =
    lens.tableCatalog(
      db,
      name,
      query,
      config,
      friendlyName
    )

  override def listAttributesQuery: Operator =
    lens.attrCatalog(
      db,
      name,
      query,
      config,
      friendlyName
    )
}