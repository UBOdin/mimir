package mimir.adaptive

import java.sql.SQLException
import scala.collection.mutable
import play.api.libs.json._
import sparsity.Name

import mimir.Database
import mimir.algebra._
import mimir.data.SystemCatalog
import mimir.serialization.AlgebraJson._
import mimir.util._
import mimir.metadata._
import com.typesafe.scalalogging.slf4j.LazyLogging

class AdaptiveSchemaManager(db: Database)
  extends LazyLogging
{
  var adaptiveSchemas = db.metadata.registerMap(
      ID("MIMIR_ADAPTIVE_SCHEMAS"), Seq(
        InitMap(Seq(
          ID("MLENS") -> TString(),
          ID("QUERY") -> TString(),
          ID("ARGS")  -> TString()
        )),
        AddColumnToMap(ID("FRIENDLY_NAME"), TString(), None)
    ))

  def create(schema: ID, mlensType: ID, query: Operator, args: Seq[Expression], humanReadableName: String) = 
  {
    val constructor:Multilens = MultilensRegistry.multilenses(mlensType)
    val config = MultilensConfig(schema, query, args, humanReadableName);
    val models = constructor.initSchema(db, config);
    
    logger.trace(s"Creating view $schema <- $mlensType(${args.mkString(", ")}")

    adaptiveSchemas.put(schema, Seq(
      StringPrimitive(mlensType.id),
      StringPrimitive(Json.toJson(query).toString),
      StringPrimitive(Json.toJson(args).toString),
      StringPrimitive(humanReadableName)
    ))

    // Persist the associated models
    for(model <- models){
      if(model.isInstanceOf[mimir.models.NeedsReconnectToDatabase])
        model.asInstanceOf[mimir.models.NeedsReconnectToDatabase].reconnectToDatabase(db)
      db.models.persist(model, ID("MULTILENS:",schema))
    }
  }

  def exists(schema: ID) = 
    adaptiveSchemas.exists(schema)

  def drop(schema: ID, ifExists: Boolean = false)
  {
    get(schema) match {
      case None if ifExists => {}
      case None => throw new SQLException(s"Adaptive schema $schema does not exist")
      case Some((mlens, config)) => {
        adaptiveSchemas.rm(schema)
        db.models.dropOwner(ID("MULTILENS:", schema))
      }

    }
  }
  def dropByName(schema: Name, ifExists: Boolean = false): Unit =
  {
    if(schema.quoted){ drop(ID(schema.name), ifExists) }
    else {
      for(comparison <- adaptiveSchemas.keys) {
        if(comparison.id.equalsIgnoreCase(schema.name)){ 
          drop(comparison, ifExists); return;
        }
      }
      if(!ifExists){ throw new SQLException(s"No such adaptive schema $schema") }
    }
  }

  def lensForRecord(record: (ID, Seq[PrimitiveValue])) =
  {
    val (name, content) = record
    val mlensType = content(0).asString
    val query = Json.parse(content(1).asString).as[Operator]
    val args:Seq[Expression] = 
      Json.parse(content(2).asString).as[Seq[Expression]]
    val humanReadableName = content(3).asString

    ( 
      MultilensRegistry.multilenses(ID(mlensType)), 
      MultilensConfig(name, query, args, humanReadableName)
    )
  }

  def all: TraversableOnce[(Multilens, MultilensConfig)] =
  {
    adaptiveSchemas.all
      .map { lensForRecord(_) }
  }
  def allProviders: TraversableOnce[(ID, AdaptiveSchemaProvider)] =
  {
    all.map { lens => (lens._2.schema, AdaptiveSchemaProvider(lens, db)) }
  }

  def allNames: TraversableOnce[ID] =
    adaptiveSchemas.keys
  
  def ofType(mlensType:ID): TraversableOnce[(Multilens, MultilensConfig)] =
  {
    adaptiveSchemas.all
      .filter { _._2(0).asString.equals(mlensType.id) }
      .map { lensForRecord(_) }
  }

  def tableCatalogs: Seq[Operator] =
  {
    all.map { case(mlens, config) => 

      val tableBaseSchemaColumns =
        SystemCatalog.tableCatalogSchema.filter(_._1 != ID("SCHEMA_NAME")).map( _._1 )

      mlens.tableCatalogFor(db, config)
        .projectByID( tableBaseSchemaColumns:_* )
        .addColumns( "SCHEMA_NAME" -> StringPrimitive(config.schema.id) )

    }.toSeq
  }
  
  def attrCatalogs: Seq[Operator] =
  {
    all.map { case(mlens, config) => 

      val attrBaseSchemaColumns =
        SystemCatalog.attrCatalogSchema.filter(_._1 != ID("SCHEMA_NAME")).map( _._1 )

      mlens.attrCatalogFor(db, config)         
        .projectByID( attrBaseSchemaColumns:_* )
        .addColumns( "SCHEMA_NAME" -> StringPrimitive(config.schema.id) )

    }.toSeq
  }

  def get(schema: ID): Option[(Multilens, MultilensConfig)] =
  {
    adaptiveSchemas.get(schema).map { lensForRecord(_) }
  }

  def getProvider(schema: ID): Option[AdaptiveSchemaProvider] =
  {
    get(schema).map { AdaptiveSchemaProvider(_, db) }
  }

  def viewFor(schema: ID, table: ID): Option[Operator] =
  {
    get(schema) match {
      case None => logger.warn(s"Invalid schema $schema"); None
      case Some((lens, config)) => {
        lens.viewFor(db, config, table) match {
          case None => None
          case Some(viewQuery) => 
            Some(AdaptiveView(
              schema,
              table, 
              viewQuery
            ))
        }
      }
    }
  }
}