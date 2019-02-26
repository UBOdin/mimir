package mimir.adaptive

import mimir.Database
import mimir.models._
import mimir.algebra._

object ShapeWatcher
  extends Multilens
{
  def initSchema(db: Database, config: MultilensConfig): TraversableOnce[Model] = ???
  def tableCatalogFor(db: Database, config: MultilensConfig): Operator = ???
  def attrCatalogFor(db: Database, config: MultilensConfig): Operator = ???
  def viewFor(db: Database, config: MultilensConfig, table: String): Option[Operator] = ???
}

