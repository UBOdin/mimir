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
            OperatorUtils.projectInColumn(
              "SOURCE", StringPrimitive("RAW"),
              db.backend.listTablesQuery
            ),
            OperatorUtils.projectInColumn(
              "SOURCE", StringPrimitive("MIMIR"),
              db.views.listViewsQuery
            )
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
            OperatorUtils.projectInColumn(
              "SOURCE", StringPrimitive("RAW"),
              db.backend.listAttrsQuery
            ),
            OperatorUtils.projectInColumn(
              "SOURCE", StringPrimitive("MIMIR"),
              db.views.listAttrsQuery
            )
          )++db.adaptiveSchemas.attrCatalogs
        )
      )
    logger.debug(s"Table View: \n$attrView")
    return attrView
  }

  def apply(name: String): Option[Operator] =
  {
    name match {
      case "MIMIR_SYS_TABLES" => Some(tableView)
      case "MIMIR_SYS_ATTRS" => Some(attrView)
      case _ => None
    }
  }


}

object SystemCatalog 
{
  val tableCatalogSchema = 
    Seq( 
      ("TABLE_NAME", TString()),
      ("SOURCE", TString()) 
    )
  val attrCatalogSchema =
    Seq( 
      ("TABLE_NAME", TString()), 
      ("ATTR_NAME", TString()),
      ("ATTR_TYPE", TString()),
      ("IS_KEY", TBool()),
      ("SOURCE", TString()) 
    )
}