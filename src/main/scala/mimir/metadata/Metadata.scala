package mimir.metadata

import mimir.algebra.{ID, Type, PrimitiveValue}

object Metadata
{
  type MapSchema = Seq[(ID, Type)]
  type MapResource = (ID, Seq[PrimitiveValue])

  def foldMapMigrations(migrations: Seq[MapMigration]): MapSchema = 
  {
    migrations.foldLeft(Seq[(ID, Type)]()) {
      case (_, InitMap(schema)) => schema
      case (prev, AddColumnToMap(column, t, _)) => prev :+ (column, t)
    }
  }
}

