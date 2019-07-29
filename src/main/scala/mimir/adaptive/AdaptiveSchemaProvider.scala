package mimir.adaptive

import mimir.Database
import mimir.algebra.{ ID, Operator, Type, AdaptiveView }
import mimir.data.ViewSchemaProvider
import mimir.exec.mode.UnannotatedBestGuess

class AdaptiveSchemaProvider(lens:Multilens, config:MultilensConfig, db: Database)
  extends ViewSchemaProvider
{
  def listTables: Seq[ID] = 
  { 
    db.query(
      listTablesQuery
          .projectByID( ID("TABLE_NAME") ),
      UnannotatedBestGuess
    ) { result => 
      result.map { row => ID(row(0).asString) }
            .toIndexedSeq 
    }
  }

  def tableSchema(table: ID): Option[Seq[(ID, Type)]] =
  {
    lens.viewFor(db, config, table)
        .map { db.typechecker.schemaOf(_) }
  }

  def view(table: ID) =
    AdaptiveView(
      config.schema,
      table, 
      lens.viewFor(db, config, table).get
    )

  override def listTablesQuery: Operator =
    lens.tableCatalogFor(db, config)

  override def listAttributesQuery: Operator =
    lens.attrCatalogFor(db, config)
}

object AdaptiveSchemaProvider
{
  def apply(lensConfig:(Multilens, MultilensConfig), db: Database) = 
    new AdaptiveSchemaProvider(lensConfig._1, lensConfig._2, db)
}