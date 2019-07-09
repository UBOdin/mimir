package mimir.metadata

import com.typesafe.scalalogging.slf4j.LazyLogging
import java.sql.SQLException
import sparsity.Name

import mimir.Database
import mimir.algebra._


class SystemCatalog(db: Database)
  extends LazyLogging
{

  // Note, we use String in this map instead of ID, since ID 
  private val simpleSchemaProviders = scala.collection.mutable.LinkedHashMap[ID, SchemaProvider]()

  def init()
  {
    // schemaProviders.put("TEMPORARY_VIEWS", db.transientViews)
    // schemaProviders.put("VIEWS", db.views)
    registerSchemaProvider(SystemCatalog.SCHEMA_NAME, this.CatalogSchemaProvider)
    // schemaProviders.put("LOADED_TABLES", db.loader)
  }

  def registerSchemaProvider(name: ID, provider: SchemaProvider)
  {
    simpleSchemaProviders.put(name, provider)
  }
  def getSchemaProvider(name: ID): SchemaProvider =
  {
    logger.warn("System Catalog does not support adaptive schemas yet")
    simpleSchemaProviders
      .get(name)
      .getOrElse { throw new SQLException(s"Invalid schema $name")}
  }
  def allSchemaProviders = {
    logger.warn("System Catalog does not support adaptive schemas yet")
    simpleSchemaProviders //TODO: ++ db.adaptiveSchemas.all
  }
  
  def tableView: Operator =
  {
    val tableView =
      OperatorUtils.makeUnion(
        allSchemaProviders.map { case (name, provider) => 
          provider.listTablesQuery
                  .addColumns( "SCHEMA_NAME" -> StringPrimitive(name.id) )
        }.toSeq
          ++ db.adaptiveSchemas.tableCatalogs
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
        allSchemaProviders.map { case (name, provider) => 
          provider.listTablesQuery
                  .addColumns( "SCHEMA_NAME" -> StringPrimitive(name.id) )
        }.toSeq
          ++ db.adaptiveSchemas.attrCatalogs
      )
      .projectByID( SystemCatalog.attrCatalogSchema.map { _._1 }:_* )

    logger.debug(s"Table View: \n$attrView")
    return attrView
  }

  // The tables themselves need to be defined lazily, since 
  // we want them read out at access time
  private val hardcodedTables = Map[ID, (Seq[(ID, Type)], () => Operator)](
    ID("SYS_TABLES")       -> ((
      SystemCatalog.tableCatalogSchema, 
      tableView _
    )),
    ID("SYS_ATTRS")        -> ((
      SystemCatalog.attrCatalogSchema, 
      attrView _
    ))
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
    for( (schema, provider) <- allSchemaProviders ) {
      provider.resolveTableCaseInsensitive(table) match {
        case None => {}
        case Some(table) => return Some((schema, table, provider))
      }
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
    for( (schema, provider) <- allSchemaProviders ) {
      if(provider.tableExists(ID(table))){ 
        return Some( (schema, ID(table), provider) ) 
      }
    }
    return None
  } 

  def resolveProviderCaseSensitive(providerName: String): Option[(ID, SchemaProvider)] = {
    logger.warn("System Catalog does not support adaptive schemas yet")
    simpleSchemaProviders
      .get(ID(providerName))
      .map { ( ID(providerName), _) }
  }

  def resolveProviderCaseInsensitive(providerName: String): Option[(ID, SchemaProvider)] = {
    for( (schema, provider) <- allSchemaProviders ) {
      if(schema.id.equalsIgnoreCase(providerName)){ 
        return Some( ( schema, provider ) )
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

  def resolveTable(providerName: Name, table: Name): Option[(ID, ID, SchemaProvider)] =
  {
    val (providerID, provider) = (
      if(provider.quoted) { resolveProviderCaseSensitive(providerName.name) }
      else { return resolveProviderCaseInsensitive(providerName.name) }
    ).getOrElse { return None }
    provider.resolveTableByName(table)
            .map { (providerID, _, provider)}
  }

  def resolveTable(providerID: ID, table: ID): Option[(ID, ID, SchemaProvider)] =
  {
    val (_, provider) = 
      resolveProviderCaseSensitive(providerID.id)
        .getOrElse { return None }

    provider.resolveTableByName(table)
            .map { (providerID, _, provider)} 
  }


  def tableExists(name: Name): Boolean = 
    resolveTable(name) != None
  def tableExists(name: ID): Boolean   = 
    resolveTable(name) != None
  def tableSchema(name: Name): Option[Seq[(ID, Type)]] = 
    resolveTable(name).flatMap { case (_, table, provider) => provider.tableSchema(table) }
  def tableSchema(name: ID): Option[Seq[(ID, Type)]]   = 
    resolveTable(name).flatMap { case (_, table, provider) => provider.tableSchema(table) }

  def tableExistsByProvider(providerName: ID, name: ID): Boolean   = 
    getSchemaProvider(providerName).tableExists(name)
  def tableSchemaByProvider(providerName: ID, name: ID): Option[Seq[(ID, Type)]]   = 
    getSchemaProvider(providerName).tableSchema(name)


  def tableOperator(defn: (ID, ID, SchemaProvider)): Operator =
    tableOperator(defn, defn._2)
  def tableOperator(defn: (ID, ID, SchemaProvider), tableAlias: ID): Operator =
  {
    val (providerName, tableName, provider) = defn
    provider.tableOperator(
      providerName, 
      tableName, 
      tableAlias
    )
  }
  def tableOperator(tableName: Name): Operator =
    tableOperator(tableName, tableName)
  def tableOperator(tableName: Name, tableAlias: Name): Operator =
    resolveTable(tableName)
      .map { tableOperator(_:(ID, ID, SchemaProvider), ID.upper(tableAlias)) }
      .getOrElse { 
        throw new SQLException(s"No such table or view '$tableName'")
      }
  def tableOperator(tableName: ID): Operator =
    tableOperator(tableName, tableName)
  def tableOperator(tableName: ID, tableAlias: ID = null): Operator =
    resolveTable(tableName)
      .map { tableOperator(_, tableAlias) }
      .getOrElse { 
        throw new SQLException(s"No such table or view '$tableName'")
      }

  /**
   * Get all availale table names 
   */
  def getAllTables(): Set[ID] =
  {
    allSchemaProviders.flatMap { _._2.listTables }
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