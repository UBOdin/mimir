package mimir.adaptive

import mimir.Database
import mimir.algebra._
import mimir.models._

case class MultilensConfig(schema: String, query: Operator, args: Seq[Expression])
{
  override def toString: String =
    s"CONFIG FOR $schema(${args.mkString(", ")})"
}

trait Multilens
{
  def initSchema(db: Database, config: MultilensConfig): TraversableOnce[Model]
  def tableCatalogFor(db: Database, config: MultilensConfig): Operator
  def attrCatalogFor(db: Database, config: MultilensConfig): Operator
  def viewFor(db: Database, config: MultilensConfig, table: String): Operator
}

