package mimir.statistics

import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir.Database
import mimir.algebra._

class SystemCatalog(db: Database)
  extends LazyLogging
{
  
  def tableView: Operator =
  {
    val tableView =
      OperatorUtils.makeUnion(
        Seq(
          db.backend.listTablesQuery
            .addColumns( "SCHEMA_NAME" -> StringPrimitive("BACKEND") ),
          db.views.listViewsQuery
            .addColumns( "SCHEMA_NAME" -> StringPrimitive("MIMIR") )
        )++db.adaptiveSchemas.tableCatalogs
      ).
      projectByID( SystemCatalog.tableCatalogSchema.map { _._1 }:_* )
    // sanity check:
    db.typechecker.schemaOf()

    logger.debug(s"Table View: \n$tableView")
    return tableView
  }
  
  def attrView: Operator =
  {
    val attrView =
      Project(
        SystemCatalog.attrCatalogSchema.map(_._1).map { col =>
          ProjectArg(col, Var(col))
        },
        OperatorUtils.makeUnion(
          Seq(
            db.backend.listAttrsQuery
              .addColumns( "SCHEMA_NAME" -> StringPrimitive("BACKEND") ),
            db.views.listAttrsQuery
              .addColumns( "SCHEMA_NAME" -> StringPrimitive("MIMIR") )
          )++db.adaptiveSchemas.attrCatalogs
        )
      )
    logger.debug(s"Table View: \n$attrView")
    return attrView
  }

  // The tables themselves need to be defined lazily, since 
  // we want them read out at access time
  private val hardcodedTables = Map[ID, () => Operator](
    ID("SYS_TABLES")       -> tableView _,
    ID("SYS_ATTRS")        -> attrView _
  )

  def apply(name: ID): Option[Operator] = hardcodedTables.get(name).map { _() }

  def list():Seq[ID] = hardcodedTables.keys.toSeq
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
}