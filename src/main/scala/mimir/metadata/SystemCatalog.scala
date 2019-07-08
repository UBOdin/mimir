package mimir.metadata

import com.typesafe.scalalogging.slf4j.LazyLogging

import sparsity.Name

import mimir.Database
import mimir.algebra._


class SystemCatalog(db: Database)
  extends LazyLogging
{
  private val simpleSchemaProviders = scala.collections.mutable.LinkedHashMap[String, SchemaProvider]

  def init()
  {
    // schemaProviders.put("TEMPORARY_VIEWS", db.transientViews)
    // schemaProviders.put("VIEWS", db.views)
    simpleSchemaProviders.put(SystemCatalog.SCHEMA_NAME, this.CatalogSchemaProvider)
    // schemaProviders.put("LOADED_TABLES", db.loader)
  }

  def registerSchemaProvider(name: ID, provider: SchemaProvider)
  {
    simpleSchemaProviders.put(name, provider)
  }
  def getSchemaProvider(name: ID): SchemaProvider =
  {
    simpleSchemaProviders
      .get(name)
      .getOrElse { throw new SQLException(s"Invalid schema $name")}
  }
  def allSchemaProviders =
    simpleSchemaProviders //TODO: ++ db.adaptiveSchemas.all
  
  def tableView: Operator =
  {
    val tableView =
      OperatorUtils.makeUnion(
        (
          schemaProviders.map { case (name, provider) => 
            provider.listTablesQuery()
                    .addColumns( "SCHEMA_NAME" -> StringPrimitive(name) )
          }
        )++db.adaptiveSchemas.tableCatalogs
      )
      .projectByID( SystemCatalog.tableCatalogSchema.map { _._1 }:_* )
    // sanity check:
    db.typechecker.schemaOf(tableView)

    logger.debug(s"Table View: \n$tableView")
    return tableView
  }
  
  def attrView: Operator =
  {
    val attrView =
      OperatorUtils.makeUnion(
        (
          schemaProviders.map { case (name, provider) => 
            provider.listTablesQuery()
                    .addColumns( "SCHEMA_NAME" -> StringPrimitive(name) )
          }
        )++db.adaptiveSchemas.attrCatalogs
      )
      .projectByID( SystemCatalog.attrCatalogSchema.map { _._1 }:_* )

    logger.debug(s"Table View: \n$attrView")
    return attrView
  }

  // The tables themselves need to be defined lazily, since 
  // we want them read out at access time
  private val hardcodedTables = Map[ID, () => (Seq[(ID, Type)], Operator)](
    ID("SYS_TABLES")       -> (
      SystemCatalog.tableCatalogSchema, 
      tableView _
    ),
    ID("SYS_ATTRS")        -> (
      SystemCatalog.attrCatalogSchema, 
      attrView _
    )
  )

  /**
   * Source a specified table with a case-insensitive name search
   * 
   * @param table   The case-insensitive name of a table
   * @return        A triple of (providerID, tableID, provider) or None 
   *                if the table doesn't exist.
   */
  def resolveTableCaseInsensitive(table: String): Option[(ID, ID, SchemaProvider)] =
  {
    for( (schema, provider) <- schemaProviders ) {
      provider.listTables
              .find { _.id.equalsIgnoreCase(table) }
              .andThen { return (schema, _, provider) }
    }
    return None
  } 

  /**
   * Source a specified table with a case-sensitive name search
   *
   * @param table   The case-sensitive name of a table
   * @return        A triple of (providerID, tableID, provider) or None 
   *                if the table doesn't exist.
   */
  def resolveTableCaseSensitive(table: String): Option[(ID, ID, SchemaProvider)] =
  {
    for( (schema, provider) <- schemaProviders ) {
      if(provider.tableExists(ID(table))){ 
        return Some( (schema, ID(table), provider) ) 
      }
    }
    return None
  } 

  def resolveTable(table: Name): Option[(ID, ID, SchemaProvider)] =
  {
    if(table.quoted) { return resolveTableCaseSensitive(table.name) }
    else { return resolveTableCaseInsensitive(table.name) }
  }

  def resolveTable(table: ID): Option[(ID, ID, SchemaProvider)] =
  {
    resolveTableCaseSensitive(table.id)
  }

  def tableExists(name: Name): Boolean = 
    resolveTable(name) != None
  def tableExists(name: ID): Boolean   = 
    resolveTable(name) != None
  def tableSchema(name: Name): Option[Seq[(ID, Type)]] = 
    resolveTable(name).flatMap { case (_, table, provider) => provider.tableSchema(table) }
  def tableSchema(name: ID): Option[Seq[(ID, Type)]]   = 
    resolveTable(name).flatMap { case (_, table, provider) => provider.tableSchema(table) }


  def tableOperator(defn: (ID, ID, SchemaProvider), tableAlias: ID): Operator =
  {
    val (providerName, tableName, provider) = defn
    provider.tableOperator(providerName, tableName, tableAlias)
  }
  def tableOperator(tableName: Name, tableAlias: Name): Operator =
    resolveTable(tableName)
      .andThen { tableOperator(_, tableAlias) }
      .getOrElse { 
        throw new SQLException(s"No such table or view '$tableName'")
      }
  def tableOperator(tableName: ID, tableAlias: ID): Operator =
    resolveTable(tableName)
      .andThen { tableOperator(_, tableAlias) }
      .getOrElse { 
        throw new SQLException(s"No such table or view '$tableName'")
      }

  /**
   * Get all availale table names 
   */
  def getAllTables(): Set[ID] =
  {
    allSchemaProviders.flatMap { _.tableList }
                      .toSet
  }

  object CatalogSchemaProvider 
    extends SchemaProvider 
  {
      def listTables = 
        hardcodedTables.keys
      def tableSchema(table: ID): Option[Seq[(ID, Type)]] = 
        hardcodedTables.get(table).map { _._1 }
      def logicalplan(table: ID) = None
      def view(table: ID) = 
        Some(hardcodedTables(table)._2())
  }
}

object SystemCatalog 
{
  val tableCatalogSchema = 
    Seq( 
      ID("SCHEMA_NAME") -> TString(),
      ID("TABLE_NAME")  -> TString()
    )
  val attrCatalogSchema =
    Seq( 
      ID("SCHEMA_NAME") -> TString(),
      ID("TABLE_NAME")  -> TString(), 
      ID("ATTR_NAME")   -> TString(),
      ID("ATTR_TYPE")   -> TString(),
      ID("IS_KEY")      -> TBool()
    )

  val SCHEMA_NAME = ID("SYSTEM_CATALOG")
}