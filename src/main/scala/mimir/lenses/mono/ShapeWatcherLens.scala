package mimir.lenses.mono

import java.sql.SQLException
import play.api.libs.json._

import mimir.Database
import mimir.algebra._
import mimir.ctables.Reason
import mimir.lenses._
import mimir.statistics.DatasetShape

object ShapeWatcherLens extends MonoLens
{
  def train(
    db: Database,
    name: ID,
    query: Operator,
    config: JsValue
  ): JsValue = 
  {
    config match {
      case JsNull => JsArray(DatasetShape.detect(db, query).map { _.toJson })
      case JsArray(_) => config
      case _ => throw new SQLException(s"Invalid lens configuration $config")
    }
  }

  def view(
    db: Database,
    name: ID,
    query: Operator,
    config: JsValue,
    friendlyName: String
  ): Operator = query

  def warnings(
    db: Database, 
    name: ID, 
    query: Operator, 
    cols: Seq[ID],
    config: JsValue, 
    friendlyName: String
  ): Seq[Reason] =
  {
    config.as[Seq[JsValue]]
          .map { DatasetShape.parse(_) }
          .flatMap { _.test(db, query) }
          .map { Reason(name, Seq(), _, false) }
  }
}
