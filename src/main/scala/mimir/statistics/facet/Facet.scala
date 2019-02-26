package mimir.statistics.facet

import mimir.Database
import mimir.algebra._

trait Facet
{
  def description: String
  def test(db:Database, query:Operator): Seq[String]
}