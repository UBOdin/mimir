package mimir.statistics.facet

import mimir.Database
import mimir.algebra._
import play.api.libs.json._

trait FacetDetector
{
  def apply(db: Database, query: Operator): Seq[Facet]
  def jsonToFacet(data: JsValue): Option[Facet]
}