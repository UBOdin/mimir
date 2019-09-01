package mimir.lenses.mono

import java.sql.SQLException
import play.api.libs.json._
import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir.Database
import mimir.algebra._
import mimir.lenses._


/**
 * A simple wrapper for CommentLens designed specifically to target CSVWithErrors
 */
object DataSourceErrors 
  extends MonoLens
  with LazyLogging
{
  val DATASOURCE_ERROR_COLUMN     = ID("_mimir_datasource_is_error")
  val DATASOURCE_ERROR_ROW_COLUMN = ID("_mimir_datasource_error_row")

  def train(
    db: Database, 
    name: ID, 
    query: Operator,
    configJson: JsValue 
  ): JsValue =
  {
    logger.debug(s"Creating DATASOURCE_ERRORS:\n${query.columnNames.mkString(", ")}\n$query")
    val columns = query.columnNames
    if(
      !columns.contains(DATASOURCE_ERROR_COLUMN)
      || !columns.contains(DATASOURCE_ERROR_ROW_COLUMN)
    ){ 
      throw new SQLException(s"Invalid DATA_SOURCE_ERRORS: Missing error definition columns: ${columns}")
    }


    Json.toJson(CommentLensConfig(
      None,
      Function(ID("concat"), Seq(
        StringPrimitive("Error parsing row ["),
        CastExpression(RowIdVar(), TString()),
        StringPrimitive("]: "),
        Var(DATASOURCE_ERROR_ROW_COLUMN)
      )),
      Some(Var(DATASOURCE_ERROR_COLUMN))
    ))
  }

  def view(
    db: Database, 
    name: ID, 
    query: Operator, 
    configJson: JsValue, 
    friendlyName: String
  ): Operator = 
    CommentLens.view(db, name, query, configJson, friendlyName)
               .removeColumnsByID( 
                  DATASOURCE_ERROR_COLUMN,
                  DATASOURCE_ERROR_ROW_COLUMN
               )
}
