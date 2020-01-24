package mimir.statistics

import play.api.libs.json._
import mimir.Database
import mimir.algebra._
import mimir.statistics.facet._

object DatasetShape
{
  val detectors = Seq[FacetDetector](
    ExpectedColumns,
    ExpectedType,
    Nullable,
    ExpectedValues
  )

  def detect(db: Database, query: Operator): Seq[Facet] =
    detectors.map { _(db, query) }.flatten

  def parse(json: JsValue): Facet =
  {
    for(d <- detectors) {
      d.jsonToFacet(json) match {
        case Some(f) => return f
        case None => ()
      }
    }
    throw new Exception(s"Invalid Json Facet Encoding: ${json}")
  }

  def parse(json: String): Facet =
    parse(Json.parse(json))
}