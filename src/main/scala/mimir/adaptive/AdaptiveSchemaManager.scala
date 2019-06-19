package mimir.adaptive

import scala.collection.mutable

import mimir.Database
import mimir.algebra._
import mimir.statistics.SystemCatalog
import mimir.serialization._
import mimir.util._
import mimir.metadata._
import com.typesafe.scalalogging.slf4j.LazyLogging

class AdaptiveSchemaManager(db: Database)
  extends LazyLogging
{
  var adaptiveSchemas: MetadataMap = null

  def init(): Unit = 
  {
    adaptiveSchemas = db.metadata.registerMap(
      ID("MIMIR_ADAPTIVE_SCHEMAS"), Seq(InitMap(Seq(
        ID("MLENS") -> TString(),
        ID("QUERY") -> TString(),
        ID("ARGS")  -> TString()
    ))))
  }

  def create(schema: ID, mlensType: ID, query: Operator, args: Seq[Expression]) = 
  {
    val constructor:Multilens = MultilensRegistry.multilenses(mlensType)
    val config = MultilensConfig(schema, query, args);
    val models = constructor.initSchema(db, config);
    
    logger.trace(s"Creating view $schema <- $mlensType(${args.mkString(", ")}")

    adaptiveSchemas.put(schema, Seq(
      StringPrimitive(mlensType.id),
      StringPrimitive(Json.ofOperator(query).toString),
      StringPrimitive(Json.ofExpressionList(args).toString)
    ))

    // Persist the associated models
    for(model <- models){
      if(model.isInstanceOf[mimir.models.NeedsReconnectToDatabase])
        model.asInstanceOf[mimir.models.NeedsReconnectToDatabase].reconnectToDatabase(db)
      db.models.persist(model, ID("MULTILENS:",schema))
    }
  }

  def drop(schema: ID, ifExists: Boolean = false)
  {
    ???
  }

  def lensForRecord(record: (ID, Seq[PrimitiveValue])) =
    record match {
      case (name, content) => 
        val mlensType = content(0).asString
        val query = Json.toOperator(Json.parse(content(1).asString))
        val args:Seq[Expression] = 
          Json.toExpressionList(Json.parse(content(2).asString))
   
        ( 
          MultilensRegistry.multilenses(ID(mlensType)), 
          MultilensConfig(ID(name.asString), query, args)
        )
    }

  def all: TraversableOnce[(Multilens, MultilensConfig)] =
  {
    adaptiveSchemas.all
      .map { lensForRecord(_) }
  }
  
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