package mimir.metadata

import java.sql._
import mimir.Database
import mimir.algebra._

abstract class MetadataBackend {

  type CategorySchema = Seq[(ID, Type)]
  type Resource = (ID, Seq[PrimitiveValue])

  def open(): Unit
  def close(): Unit

  def register(category: ID, schema: CategorySchema): Unit
  def all(category: ID): Seq[Resource]
  def get(category: ID, resource: ID): Resource
  def put(category: ID, resource: Resource): Unit

}
