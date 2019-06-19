package mimir.metadata

import java.sql._
import mimir.Database
import mimir.algebra._

object Metadata
{
  type MapSchema = Seq[(ID, Type)]
  type MapResource = (ID, Seq[PrimitiveValue])
}

abstract class MetadataBackend {


  def open(): Unit
  def close(): Unit

  def registerMap(category: ID, schema: Metadata.MapSchema): MetadataMap
  def registerManyMany(category: ID): MetadataManyMany

  def keysForMap(category: ID): Seq[ID]
  def allForMap(category: ID): Seq[Metadata.MapResource]
  def getFromMap(category: ID, resource: ID): Option[Metadata.MapResource]
  def putToMap(category: ID, resource: Metadata.MapResource): Unit
  def rmFromMap(category: ID, resource: ID): Unit
  def updateMap(category: ID, resource: ID, body:Map[ID, PrimitiveValue]): Unit

  def addToManyMany(category: ID, lhs:ID, rhs: ID): Unit
  def getManyManyByLHS(category: ID, lhs:ID): Seq[ID]
  def getManyManyByRHS(category: ID, rhs:ID): Seq[ID]
  def rmFromManyMany(category: ID, lhs:ID, rhs: ID): Unit
  def rmByLHSFromManyMany(category: ID, lhs: ID): Unit
  def rmByRHSFromManyMany(category: ID, rhs: ID): Unit
}

class MetadataMap(backend: MetadataBackend, category: ID)
{
  def keys: Seq[ID]                                  = backend.keysForMap(category)
  def all: Seq[Metadata.MapResource]                 = backend.allForMap(category)
  def get(resource:ID): Option[Metadata.MapResource] = backend.getFromMap(category, resource)
  def put(resource: Metadata.MapResource)            = backend.putToMap(category, resource)
  def put(id: ID, body: Seq[PrimitiveValue])         = backend.putToMap(category, (id, body))
  def update(id: ID, body: Map[ID, PrimitiveValue])  = backend.updateMap(category, id, body)
  def rm(resource: ID)                               = backend.rmFromMap(category, resource)
}

class MetadataManyMany(backend: MetadataBackend, category: ID)
{
  def add(lhs: ID, rhs: ID)      = backend.addToManyMany(category, lhs, rhs)
  def getByLHS(lhs: ID): Seq[ID] = backend.getManyManyByLHS(category, lhs)
  def getByRHS(rhs: ID): Seq[ID] = backend.getManyManyByRHS(category, rhs)
  def rm(lhs: ID, rhs: ID)       = backend.rmFromManyMany(category, lhs, rhs)
  def rmByLHS(lhs: ID)           = backend.rmByLHSFromManyMany(category, lhs)
  def rmByRHS(rhs: ID)           = backend.rmByRHSFromManyMany(category, rhs)
}