package mimir.data

import com.typesafe.scalalogging.slf4j.LazyLogging
import java.sql.SQLException
import sparsity.Name
import play.api.libs.json._

import mimir.Database
import mimir.algebra._
import mimir.data._
import mimir.views.{ ViewManager, TemporaryViewManager }
import mimir.util.ExperimentalOptions
import mimir.exec.spark.MimirSpark
import mimir.metadata.MetadataManyMany
import mimir.ctables.CoarseDependency
import mimir.lenses.LensManager

class SystemCatalog(db: Database)
  extends LazyLogging
{

  // Note, we use String in this map instead of ID, since ID 
  private val simpleSchemaProviders = scala.collection.mutable.LinkedHashMap[ID, SchemaProvider]()
  private var preferredMaterializedTableProvider: ID = null
  val coarseDependencies = db.metadata.registerManyMany(
                              ID("MIMIR_COARSE_DEPENDENCIES")
                            )

  {
    // The order in which the schema providers are registered is the order
    // in which they're used to resolve table names.

    // Existing restrictions / assumptions on this order include:
    // 
    // --- ViewManager must come BEFORE LoadedTables --- 
    //   db.loader.loadTable creates both a LoadedTable and a View with the
    //   same name so that users can access both the actual data, as well as
    //   the post-processed view.  Generally, we want users to see the 
    //   post-processed version by default.
    // 
    // --- SparkSchemaProvider must come BEFORE LoadedTables ---
    //   This is necessary for Spark to take priority over LoadedTables
    //   for the preferredBulkSchemaProvider.
    //
    // --- CatalogSchemaProvider *should* come LAST ---
    //   Not strictly necessary, but the two tables defined by this
    //   provider use relatively common names.  Really it shouldn't
    //   even be part of the normal search path, but eh?
    // 
    registerSchemaProvider(TemporaryViewManager.SCHEMA, db.tempViews)
    registerSchemaProvider(ViewManager.SCHEMA, db.views)
    registerSchemaProvider(LensManager.SCHEMA, db.lenses)
    if(ExperimentalOptions.isEnabled("USE-DERBY") || MimirSpark.remoteSpark){
      registerSchemaProvider(ID("SPARK"), new SparkSchemaProvider(db))
    }
    registerSchemaProvider(LoadedTables.SCHEMA, db.loader)
    registerSchemaProvider(SystemCatalog.SCHEMA, this.CatalogSchemaProvider)
  }

  def registerSchemaProvider(name: ID, provider: SchemaProvider)
  {
    simpleSchemaProviders.put(name, provider)
  }
  def getSchemaProvider(name: ID): SchemaProvider =
  {
    simpleSchemaProviders
      .get(name)
      .getOrElse { 
        db.adaptiveSchemas
          .getProvider(name)
          .getOrElse { throw new SQLException(s"Invalid schema $name") }
      }
  }
  def allSchemaProviders: Seq[(ID, SchemaProvider)] = {
    simpleSchemaProviders.toSeq ++ db.adaptiveSchemas.allProviders
  }
  
  def tableView: Operator =
  {
    val tableView =
      OperatorUtils.makeUnion(
        allSchemaProviders
          .filter { _._2.isVisible }
          .map { case (name, provider) => 
            provider.listTablesQuery
                    .addColumns( "SCHEMA_NAME" -> StringPrimitive(name.id) )
          }.toSeq
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
        allSchemaProviders
          .filter { _._2.isVisible }
          .map { case (name, provider) => 
            provider.listAttributesQuery
                    .addColumns( "SCHEMA_NAME" -> StringPrimitive(name.id) )
          }.toSeq
      )
      .projectByID( SystemCatalog.attrCatalogSchema.map { _._1 }:_* )

    logger.debug(s"Table View: \n$attrView")
    return attrView
  }

  // The tables themselves need to be defined lazily, since 
  // we want them read out at access time
  private val hardcodedTables = Map[ID, (Seq[(ID, Type)], () => Operator)](
    ID("TABLES")       -> ((
      SystemCatalog.tableCatalogSchema, 
      tableView _
    )),
    ID("ATTRIBUTES")   -> ((
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
    logger.debug(s"Resolve table (Case INsensitive): $table")
    for( (schema, provider) <- allSchemaProviders ) {
      logger.trace(s"Trying for $table in $schema")
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
    logger.debug(s"Resolve table (Case Sensitive): $table")
    for( (schema, provider) <- allSchemaProviders ) {
      logger.trace(s"Trying for $table in $schema")
      if(provider.tableExists(ID(table))){ 
        return Some( (schema, ID(table), provider) ) 
      }
    }
    return None
  } 

  def resolveProviderCaseSensitive(providerName: String): Option[(ID, SchemaProvider)] = {
    simpleSchemaProviders
      .get(ID(providerName))
      .orElse { db.adaptiveSchemas.getProvider(ID(providerName)) }
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
      if(providerName.quoted) { resolveProviderCaseSensitive(providerName.name) }
      else { resolveProviderCaseInsensitive(providerName.name) }
    ).getOrElse { return None }
    provider.resolveTableByName(table)
            .map { (providerID, _, provider)}
  }

  def resolveTable(providerNameMaybe: Option[Name], table: Name): Option[(ID, ID, SchemaProvider)] =
  {
    providerNameMaybe match {
      case None => resolveTable(table)
      case Some(providerName) => resolveTable(providerName, table)
    }
  }

  def resolveTable(providerID: ID, table: ID): Option[(ID, ID, SchemaProvider)] =
  {
    val (_, provider) = 
      resolveProviderCaseSensitive(providerID.id)
        .getOrElse { return None }

    if(provider.tableExists(table)){
      return Some( (providerID, table, provider) )
    } else { return None }
  }


  def tableExists(name: Name): Boolean = 
    resolveTable(name) != None
  def tableExists(name: ID): Boolean   = 
    resolveTable(name) != None
  def tableExists(providerName: Name, name: Name): Boolean   = 
    provider(providerName)
      .getOrElse { return false }
      ._2
      .resolveTableByName(name) != None
  def tableExists(providerName: ID, name: ID): Boolean   = 
    getSchemaProvider(providerName).tableExists(name)

  def tableSchema(name: Name): Option[Seq[(ID, Type)]] = 
    resolveTable(name).flatMap { case (_, table, provider) => provider.tableSchema(table) }
  def tableSchema(name: ID): Option[Seq[(ID, Type)]]   = 
    resolveTable(name).flatMap { case (_, table, provider) => provider.tableSchema(table) }
  def tableSchema(providerName: Name, name: Name): Option[Seq[(ID, Type)]] = 
    provider(providerName).flatMap { case (_, providerImpl) => 
      providerImpl.resolveTableByName(name).flatMap { providerImpl.tableSchema(_)} }
  def tableSchema(providerName: ID, name: ID): Option[Seq[(ID, Type)]]   = 
    provider(providerName).flatMap { _._2.tableSchema(name) }




  def tableOperator(defn: (ID, ID, SchemaProvider)): Operator =
  {
    val (providerName, tableName, provider) = defn
    provider.tableOperator(
      providerName, 
      tableName
    )
  }
  def tableOperator(tableName: Name): Operator =
    resolveTable(tableName)
      .map { tableOperator(_:(ID, ID, SchemaProvider)) }
      .getOrElse { 
        throw new SQLException(s"No such table or view '$tableName'")
      }
  def tableOperator(tableName: ID): Operator =
    resolveTable(tableName)
      .map { tableOperator(_) }
      .getOrElse { 
        throw new SQLException(s"No such table or view '$tableName'")
      }
  def tableOperator(providerName: Name, tableName: Name): Operator =
    tableOperator(
      resolveTable(providerName, tableName).getOrElse {
        throw new SQLException(s"No such table or view '$providerName.$tableName'")
      })
  def tableOperator(providerName: ID, tableName: ID): Operator =
    tableOperator(
      resolveTable(providerName, tableName).getOrElse {
        throw new SQLException(s"No such table or view '$providerName.$tableName'")
      } )

  def provider(providerName: Name): Option[(ID, SchemaProvider)] =
    if(providerName.quoted){ resolveProviderCaseSensitive(providerName.name) }
    else { resolveProviderCaseInsensitive(providerName.name) }
  def provider(providerName: ID): Option[(ID, SchemaProvider)] =
    resolveProviderCaseSensitive(providerName.id)

  def materializedTableProvider(providerName: ID = null): SchemaProvider with MaterializedTableProvider =
  {
    var targetProvider = providerName
    if(targetProvider == null){ targetProvider = preferredMaterializedTableProvider }
    if(targetProvider != null){
      simpleSchemaProviders.get(targetProvider) match {
        case None => throw new SQLException(s"'$targetProvider' is not a registered schema provider.")
        case Some(s: SchemaProvider with MaterializedTableProvider) => return s
        case _ => throw new SQLException(s"'$targetProvider' is not a valid bulk storage provider.")
      }
    }
    simpleSchemaProviders.foreach { 
      case (validProviderName, provider: SchemaProvider with MaterializedTableProvider) => {
          preferredMaterializedTableProvider = validProviderName
          return provider
        }
      case _ => ()
    }
    throw new SQLException("No registered schema providers support bulk storage.")
  }
  def materializedTableProviderID: ID =
  {
    if(preferredMaterializedTableProvider != null) { return preferredMaterializedTableProvider }
    materializedTableProvider()
    return preferredMaterializedTableProvider;
  }


  /**
   * Get all availale table names 
   */
  def getAllTables(): Set[ID] =
  {
    allSchemaProviders.flatMap { _._2.listTables }
                      .toSet
  }

  private def safeAssembleIdentifierPair(pair: (ID, ID)): ID = 
  {
    ID(Seq(pair._1, pair._2).map { 
      _.id
       .replaceAll("[\\\\]", "\\\\")
       .replaceAll("[.]", "\\\\.")
    }.mkString("."))
  }

  /**
   * Register a coarse-grained provenance relationship.
   * @param   target    The table affected by the provenance relationship
   * @param   source    The schema/table from which [target] received input
   *
   * Support for coarse-grained provenance relationships between tables.
   * In general, we want all inter-table relationships to be given explicitly
   * through views.  However, in some cases, it becomes necessary to pipe
   * a table through an external process like a python script.  See:
   * - https://github.com/VizierDB/web-ui/issues/116
   * - https://github.com/UBOdin/mimir/issues/319
   * In these cases, we still want to preserve the relationship between
   * the table(s) read by the external process and the tables written out
   * by the external process to propagate VGTerm/DataWarnings through.
   * 
   * This function registers such a relationship. 
   */
  def createDependency(target: (ID, ID), source: CoarseDependency)
  {
    coarseDependencies.add(
      safeAssembleIdentifierPair(target), 
      ID(Json.stringify(Json.toJson(source)))
    )
  }

  /**
   * Deregister a coarse-grained provenance relationship.
   * @param   target    The table affected by the provenance relationship
   * @param   source    The schema/table from which [target] received input
   * 
   * See the discussion of addProvenance
   */
  def dropDependency(target: (ID, ID), source: CoarseDependency)
  {
    coarseDependencies.rm(
      safeAssembleIdentifierPair(target), 
      ID(Json.stringify(Json.toJson(source)))
    )
  }

  /**
   * Retrieve all coarse-grained provenance relationships for a table.
   * @param   target    The table affected by the provenance relationship
   * @param   source    The schema/table from which [target] received input
   * 
   * See the discussion of addProvenance
   */
  def getDependencies(target: (ID, ID)): Seq[CoarseDependency] =
  {
    coarseDependencies.getByLHS(
      safeAssembleIdentifierPair(target)
    ).map { dep =>
      Json.parse(dep.id).as[CoarseDependency]
    }
  }


  object CatalogSchemaProvider 
    extends ViewSchemaProvider 
  {
      def listTables = 
        hardcodedTables.keys
      def tableSchema(table: ID): Option[Seq[(ID, Type)]] = 
        hardcodedTables.get(table).map { _._1 }
      def view(table: ID) = 
        hardcodedTables(table)._2()
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

  val SCHEMA = ID("SYSTEM")
}