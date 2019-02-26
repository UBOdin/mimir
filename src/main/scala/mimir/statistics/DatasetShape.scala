package mimir.statistics

import mimir.Database
import mimir.algebra._
import mimir.statistics.facet._

object DatasetShape
{
  val detectors = Seq[(Database, Operator) => Seq[Facet]](
    ExpectedColumns(_,_),
    ExpectedType(_,_)
  )

  def detect(db: Database, query: Operator): Seq[Facet] =
    detectors.map { _(db, query) }.flatten
}