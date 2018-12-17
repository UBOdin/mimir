package mimir.adaptive

import scala.collection.mutable

import mimir.Database
import mimir.algebra._
import mimir.statistics.SystemCatalog
import mimir.serialization._
import mimir.util._

class AdaptiveSchemaManager(db: Database)
{
  val dataTable = "MIMIR_ADAPTIVE_SCHEMAS"

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

  def create(schema: String, mlensType: String, query: Operator, args: Seq[Expression]) = 
  {
    val constructor:Multilens = MultilensRegistry.multilenses(mlensType)
    val config = MultilensConfig(schema, query, args);
    val models = constructor.initSchema(db, config);
    
    db.metadataBackend.update(s"""
      INSERT INTO $dataTable(NAME, MLENS, QUERY, ARGS) VALUES (?,?,?,?)
    """, Seq(
      StringPrimitive(schema),
      StringPrimitive(mlensType),
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

  def all: TraversableOnce[(Multilens, MultilensConfig)] =
  {
    db.queryMetadata(
      Project(
        Seq(ProjectArg("NAME", Var("NAME")), 
            ProjectArg("MLENS", Var("MLENS")), 
            ProjectArg("QUERY", Var("QUERY")),
            ProjectArg("ARGS", Var("ARGS"))),
        db.metadataTable(dataTable)
      )
    ){ _.map { row => 
      val name = row(0).asString
      val mlensType = row(1).asString
      val query = Json.toOperator(Json.parse(row(2).asString), db.types)
      val args:Seq[Expression] = 
        Json.toExpressionList(Json.parse(row(3).asString), db.types)
 
      ( 
        MultilensRegistry.multilenses(mlensType), 
        MultilensConfig(name, query, args)
      )
    }.toIndexedSeq }
  }
  
  def some(mlensType:String): TraversableOnce[(Multilens, MultilensConfig)] =
  {
    db.queryMetadata(
      Select(Comparison(Cmp.Eq, Var("MLENS"), StringPrimitive(mlensType)), Project(
        Seq(ProjectArg("NAME", Var("NAME")), 
            ProjectArg("MLENS", Var("MLENS")), 
            ProjectArg("QUERY", Var("QUERY")),
            ProjectArg("ARGS", Var("ARGS"))),
        db.metadataTable(dataTable)
      ))
    ){ _.map { row => 
      val name = row(0).asString
      val mlensType = row(1).asString
      val query = Json.toOperator(Json.parse(row(2).asString), db.types)
      val args:Seq[Expression] = 
        Json.toExpressionList(Json.parse(row(3).asString), db.types)
 
      ( 
        MultilensRegistry.multilenses(mlensType), 
        MultilensConfig(name, query, args)
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
        .addColumn( "SCHEMA_NAME" -> StringPrimitive(config.schema) )

    }.toSeq
  }

  def tableCatalogs(mlensType:String): Seq[Operator] =
  {
    some(mlensType).map { case(mlens, config) => 

      val tableBaseSchemaColumns =
        SystemCatalog.tableCatalogSchema.filter(_._1 != "SCHEMA_NAME").map( _._1 )

      mlens.tableCatalogFor(db, config)
        .project( tableBaseSchemaColumns:_* )
        .addColumn( "SCHEMA_NAME" -> StringPrimitive(config.schema) )

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
  
  def attrCatalogs(mlensType:String): Seq[Operator] =
  {
    some(mlensType).map { case(mlens, config) => 

      val attrBaseSchemaColumns =
        SystemCatalog.attrCatalogSchema.filter(_._1 != "SCHEMA_NAME").map( _._1 )

      mlens.attrCatalogFor(db, config)         
        .project( attrBaseSchemaColumns:_* )
        .addColumn( "SCHEMA_NAME" -> StringPrimitive(config.schema) )

    }.toSeq
  }

  def get(schema: String): Option[(Multilens, MultilensConfig)] =
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
        val query = Json.toOperator(Json.parse(row(2).asString), db.types)
        val args:Seq[Expression] = 
          Json.toExpressionList(Json.parse(row(3).asString), db.types)
   
        Some(( 
          MultilensRegistry.multilenses(mlensType), 
          MultilensConfig(name, query, args)
        ))
      } else { None }
    }
  }

  def viewFor(schema: String, table: String): Option[Operator] =
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