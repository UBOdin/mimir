package mimir.statistics.facet

trait FacetDetector
{
  def detect(db: Database, query: Operator)
}