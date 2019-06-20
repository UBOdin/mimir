package mimir.metadata

import mimir.algebra.{ID, Type, PrimitiveValue}

sealed trait MapMigration

case class InitMap(schema: Metadata.MapSchema) extends MapMigration
case class AddColumnToMap(column: ID, t: Type, default: Option[PrimitiveValue]) extends MapMigration