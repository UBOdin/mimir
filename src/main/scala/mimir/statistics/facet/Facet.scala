package mimir.statistics.facet

import play.api.libs.json._
import mimir.Database
import mimir.algebra._
import mimir.statistics.DatasetShape

trait Facet
{
  def description: String
  def test(db:Database, query:Operator): Seq[String]
  def toJson: JsValue

  override def toString = description
}

object Facet
{
  def parse(json: JsValue): Facet =
  {
    for(d <- DatasetShape.detectors) {
      d.jsonToFacet(json) match {
        case Some(f) => return f
        case None => ()
      }
    }
    throw new Exception(s"Invalid Json Facet Encoding: ${json}")
  }

  def parse(json: String): Facet =
    parse(Json.parse(json))

  implicit val format: Format[Facet] = Format(
    JsPath.read[JsObject].map { parse(_) },
    new Writes[Facet] { def writes(f: Facet) = f.toJson }
  )
}

trait AppliesToColumn
  extends Facet
{
  def facetInvalidCondition: Expression
  def appliesToColumn: ID
  def facetInvalidDescription: Expression
}

trait AppliesToRow
  extends Facet
{
  def facetInvalidCondition: Expression
  def facetInvalidDescription: Expression
}