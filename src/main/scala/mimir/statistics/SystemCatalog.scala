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
      Project(
        SystemCatalog.tableCatalogSchema.map(_._1).map { col =>
          ProjectArg(col, Var(col))
        },
        OperatorUtils.makeUnion(
          Seq(
            db.backend.listTablesQuery
              .addColumn( "SCHEMA_NAME" -> StringPrimitive("BACKEND") ),
            db.views.listViewsQuery
              .addColumn( "SCHEMA_NAME" -> StringPrimitive("MIMIR") )
          )++db.adaptiveSchemas.tableCatalogs
        )
      )
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
              .addColumn( "SCHEMA_NAME" -> StringPrimitive("BACKEND") ),
            db.views.listAttrsQuery
              .addColumn( "SCHEMA_NAME" -> StringPrimitive("MIMIR") )
          )++db.adaptiveSchemas.attrCatalogs
        )
      )
    logger.debug(s"Table View: \n$attrView")
    return attrView
  }

  def apply(name: String): Option[Operator] =
  {
    name match {
      case ("MIMIR_SYS_TABLES" | "SYS_TABLES") => Some(tableView)
      case ("MIMIR_SYS_ATTRS"  | "SYS_ATTRS" ) => Some(attrView)
      case _ => None
    }
  }


}

object SystemCatalog 
{
  val tableCatalogSchema = 
    Seq( 
      ("TABLE_NAME", TString()),
      ("SCHEMA_NAME", TString()) 
    )
  val attrCatalogSchema =
    Seq( 
      ("TABLE_NAME", TString()), 
      ("ATTR_NAME", TString()),
      ("ATTR_TYPE", TString()),
      ("IS_KEY", TBool()),
      ("SCHEMA_NAME", TString()) 
    )
}