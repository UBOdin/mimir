package mimir.statistics.facet

import play.api.libs.json._
import mimir.Database
import mimir.algebra._

trait Facet
{
  def description: String
  def test(db:Database, query:Operator): Seq[String]
  def toJson: JsValue

  override def toString = description
}