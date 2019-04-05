package mimir.adaptive

import scala.collection.mutable

import mimir.Database
import mimir.algebra._
import mimir.statistics.SystemCatalog
import mimir.serialization._
import mimir.util._
import com.typesafe.scalalogging.slf4j.LazyLogging

class AdaptiveSchemaManager(db: Database)
  extends LazyLogging
{
  val dataTable = ID("MIMIR_ADAPTIVE_SCHEMAS")

  def init(): Unit = 
  {
    if(db.metadataBackend.getTableSchema(dataTable).isEmpty){
      db.metadataBackend.update(s"""
        CREATE TABLE $dataTable(
          NAME varchar(100), 
          MLENS varchar(100),
          QUERY text,
          ARGS text,
          PRIMARY KEY (name)
        )
      """)
    }
  }

  def create(schema: ID, mlensType: ID, query: Operator, args: Seq[Expression]) = 
  {
    val constructor:Multilens = MultilensRegistry.multilenses(mlensType)
    val config = MultilensConfig(schema, query, args);
    val models = constructor.initSchema(db, config);
    
    logger.trace(s"Creating view $schema <- $mlensType(${args.mkString(", ")}")

    db.metadataBackend.update(s"""
      INSERT INTO $dataTable(NAME, MLENS, QUERY, ARGS) VALUES (?,?,?,?)
    """, Seq(
      StringPrimitive(schema.id),
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

  def all: TraversableOnce[(Multilens, MultilensConfig)] =
  {
    db.queryMetadata(
      db.metadataTable(dataTable)
        .project("NAME", "MLENS", "QUERY", "ARGS")
    ){ _.map { row => 
      val name = row(0).asString
      val mlensType = row(1).asString
      val query = Json.toOperator(Json.parse(row(2).asString))
      val args:Seq[Expression] = 
        Json.toExpressionList(Json.parse(row(3).asString))
 
      ( 
        MultilensRegistry.multilenses(ID(mlensType)), 
        MultilensConfig(ID(name), query, args)
      )
    }.toIndexedSeq }
  }
  
  def some(mlensType:ID): TraversableOnce[(Multilens, MultilensConfig)] =
  {
    db.queryMetadata(
      db.metadataTable(dataTable)
        .filter { Var(ID("MLENS")).eq(StringPrimitive(mlensType.id)) }
        .project("NAME", "MLENS", "QUERY", "ARGS")
    ){ _.map { row => 
      val name = row(0).asString
      val mlensType = row(1).asString
      val query = Json.toOperator(Json.parse(row(2).asString))
      val args:Seq[Expression] = 
        Json.toExpressionList(Json.parse(row(3).asString))
 
      ( 
        MultilensRegistry.multilenses(ID(mlensType)), 
        MultilensConfig(ID(name), query, args)
      )
    }.toIndexedSeq }
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

  def tableCatalogs(mlensType:ID): Seq[Operator] =
  {
    some(mlensType).map { case(mlens, config) => 

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
  
  def attrCatalogs(mlensType:ID): Seq[Operator] =
  {
    some(mlensType).map { case(mlens, config) => 

      val attrBaseSchemaColumns =
        SystemCatalog.attrCatalogSchema.filter(_._1 != ID("SCHEMA_NAME")).map( _._1 )

      mlens.attrCatalogFor(db, config)         
        .projectByID( attrBaseSchemaColumns:_* )
        .addColumns( "SCHEMA_NAME" -> StringPrimitive(config.schema.id) )

    }.toSeq
  }

  def get(schema: ID): Option[(Multilens, MultilensConfig)] =
  {
    db.queryMetadata(
      Select(
        Comparison(Cmp.Eq, Var(ID("NAME")), StringPrimitive(schema.id)),
        db.metadataTable(dataTable)
      )
    ){ result =>
      if(result.hasNext){
        val row = result.next
        val name = ID(row(0).asString)
        val mlensType = ID(row(1).asString)
        val query = Json.toOperator(Json.parse(row(2).asString))
        val args:Seq[Expression] = 
          Json.toExpressionList(Json.parse(row(3).asString))
   
        Some(( 
          MultilensRegistry.multilenses(mlensType), 
          MultilensConfig(name, query, args)
        ))
      } else { None }
    }
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