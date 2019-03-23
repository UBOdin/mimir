package mimir.adaptive

import mimir.Database
import mimir.algebra._
import mimir.models._

case class MultilensConfig(schema: ID, query: Operator, args: Seq[Expression])
{
  override def toString: String =
    s"CONFIG FOR $schema(${args.mkString(", ")})"
}

trait Multilens
{
  /**
   * Initialize the Adaptive Schema: Initialize any required models, and populate any relevant backend tables.
   */
  def initSchema(db: Database, config: MultilensConfig): TraversableOnce[Model]
  /**
   * Return an operator that computes a list of tables.  The operator should have the schema:
   * - TABLE_NAME : TString   --- The name of the table
   */
  def tableCatalogFor(db: Database, config: MultilensConfig): Operator

  /**
   * Return an operator that computes a list of attributes in all tables.  The operator should have the schema:
   * - TABLE_NAME : TString   --- The name of the table that the attribute belongs to
   * - ATTR_NAME : TString    --- The name of the attribute itself
   * - ATTR_TYPE : TString    --- The type of the attribute
   * - IS_KEY : TBool         --- TRUE if the attribute is part of the primary key for the table
   */
  def attrCatalogFor(db: Database, config: MultilensConfig): Operator

  /**
   * Return the view operator for the specified table.  This operator should have a
   * schema consistent with the best-guess for attrCatalogFor.
   */
  def viewFor(db: Database, config: MultilensConfig, table: ID): Option[Operator]
}

