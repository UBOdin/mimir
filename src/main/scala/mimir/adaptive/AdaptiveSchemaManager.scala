package mimir.adaptive

import scala.collection.mutable
import sparsity.Name

import mimir.Database
import mimir.algebra._
import mimir.statistics.SystemCatalog
import mimir.serialization._
import mimir.util._

class AdaptiveSchemaManager(db: Database)
{
  val dataTable = Name("MIMIR_ADAPTIVE_SCHEMAS")

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

  def create(schema: Name, mlensType: Name, query: Operator, args: Seq[Expression]) = 
  {
    val constructor:Multilens = MultilensRegistry.multilenses(mlensType)
    val config = MultilensConfig(schema, query, args);
    val models = constructor.initSchema(db, config);
    
    db.metadataBackend.update(s"""
      INSERT INTO $dataTable(NAME, MLENS, QUERY, ARGS) VALUES (?,?,?,?)
    """, Seq(
      StringPrimitive(schema.upper),
      StringPrimitive(mlensType.upper),
      StringPrimitive(Json.ofOperator(query).toString),
      StringPrimitive(Json.ofExpressionList(args).toString)
    ))

    // Persist the associated models
    for(model <- models){
      if(model.isInstanceOf[mimir.models.NeedsReconnectToDatabase])
        model.asInstanceOf[mimir.models.NeedsReconnectToDatabase].reconnectToDatabase(db)
      db.models.persist(model, s"MULTILENS:$schema")
    }
  }

  def drop(schema: Name, ifExists: Boolean = false)
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
        MultilensRegistry.multilenses(Name(mlensType)), 
        MultilensConfig(Name(name), query, args)
      )
    }.toIndexedSeq }
  }
  
  def some(mlensType:Name): TraversableOnce[(Multilens, MultilensConfig)] =
  {
    db.queryMetadata(
      db.metadataTable(dataTable)
        .filter { Var(Name("MLENS")).eq(StringPrimitive(mlensType.upper)) }
        .project("NAME", "MLENS", "QUERY", "ARGS")
    ){ _.map { row => 
      val name = row(0).asString
      val mlensType = row(1).asString
      val query = Json.toOperator(Json.parse(row(2).asString))
      val args:Seq[Expression] = 
        Json.toExpressionList(Json.parse(row(3).asString))
 
      ( 
        MultilensRegistry.multilenses(Name(mlensType)), 
        MultilensConfig(Name(name), query, args)
      )
    }.toIndexedSeq }
  }

  def tableCatalogs: Seq[Operator] =
  {
    all.map { case(mlens, config) => 

      val tableBaseSchemaColumns =
        SystemCatalog.tableCatalogSchema.filter(_._1 != "SCHEMA_NAME").map( _._1 )

      mlens.tableCatalogFor(db, config)
        .project( tableBaseSchemaColumns:_* )
        .addColumn( "SCHEMA_NAME" -> StringPrimitive(config.schema.upper) )

    }.toSeq
  }

  def tableCatalogs(mlensType:Name): Seq[Operator] =
  {
    some(mlensType).map { case(mlens, config) => 

      val tableBaseSchemaColumns =
        SystemCatalog.tableCatalogSchema.filter(_._1 != "SCHEMA_NAME").map( _._1 )

      mlens.tableCatalogFor(db, config)
        .project( tableBaseSchemaColumns:_* )
        .addColumn( "SCHEMA_NAME" -> StringPrimitive(config.schema.upper) )

    }.toSeq
  }
  
  def attrCatalogs: Seq[Operator] =
  {
    all.map { case(mlens, config) => 

      val attrBaseSchemaColumns =
        SystemCatalog.attrCatalogSchema.filter(_._1 != "SCHEMA_NAME").map( _._1 )

      mlens.attrCatalogFor(db, config)         
        .project( attrBaseSchemaColumns:_* )
        .addColumn( "SCHEMA_NAME" -> StringPrimitive(config.schema) )

    }.toSeq
  }
  
  def attrCatalogs(mlensType:Name): Seq[Operator] =
  {
    some(mlensType).map { case(mlens, config) => 

      val attrBaseSchemaColumns =
        SystemCatalog.attrCatalogSchema.filter(_._1 != "SCHEMA_NAME").map( _._1 )

      mlens.attrCatalogFor(db, config)         
        .project( attrBaseSchemaColumns:_* )
        .addColumn( "SCHEMA_NAME" -> StringPrimitive(config.schema) )

    }.toSeq
  }

  def get(schema: Name): Option[(Multilens, MultilensConfig)] =
  {
    db.queryMetadata(
      Select(
        Comparison(Cmp.Eq, Var("NAME"), StringPrimitive(schema)),
        db.metadataTable(dataTable)
      )
    ){ result =>
      if(result.hasNext){
        val row = result.next
        val name = row(0).asString
        val mlensType = row(1).asString
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

  def viewFor(schema: Name, table: Name): Option[Operator] =
  {
    get(schema) match {
      case None => None
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