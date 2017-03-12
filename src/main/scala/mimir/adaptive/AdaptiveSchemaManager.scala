package mimir.adaptive

import scala.collection.mutable

import mimir.Database
import mimir.algebra._

class AdaptiveSchemaManager(db: Database)
{
  val dataTable = "MIMIR_ADAPTIVE_SCHEMAS"

  def init(): Unit = 
  {
    if(db.backend.getTableSchema(dataTable).isEmpty){
      db.backend.update(s"""
        CREATE TABLE $dataTable(
          NAME varchar(100), 
          MLENS_TYPE varchar(100),
          QUERY text,
          ARGS text,
          PRIMARY KEY (name)
        )
      """)
    }
  }

  def create(schema: String, mlensType: String, query: Operator, args: Seq[Expression]) = 
  {
    val lens = MultilensRegistry.multilenses(mlensType)
    val lensArgs = MultilensConfig(schema, query, args);
    val lensModels = lens.initSchema(db, lensArgs);
    db.backend.update(s"""
      INSERT INTO $dataTable(NAME, MLENS_TYPE, QUERY, ARGS) VALUES (?,?,?,?)
    """, Seq(
      StringPrimitive(schema),
      StringPrimitive(mlensType),
      StringPrimitive(db.querySerializer.serialize(query)),
      StringPrimitive(args.map(db.querySerializer.serialize(_)).mkString("~"))
    ))
  }

  def all: TraversableOnce[(Multilens, MultilensConfig)] =
  {
    db.query(
      db.getTableOperator(dataTable)
    ).mapRows { row => 
      val name = row(0).asString
      val mlensType = row(1).asString
      val query = db.querySerializer.deserializeQuery(row(2).asString)
      val args:Seq[Expression] = 
        if(row(3).equals(StringPrimitive(""))) { Seq() }
        else { row(3).asString.split("~").map( db.querySerializer.deserializeExpression(_) ) }
 
      ( 
        MultilensRegistry.multilenses(mlensType), 
        MultilensConfig(name, query, args)
      )
    }
  }

  def attrCatalogs: Seq[Operator] =
  {
    all.map { case(mlens, config) => mlens.attrCatalogFor(db, config) }.toSeq
  }

  def tableCatalogs: Seq[Operator] =
  {
    all.map { case(mlens, config) => mlens.attrCatalogFor(db, config) }.toSeq
  }
}