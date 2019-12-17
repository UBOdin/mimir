package mimir.exec.spark

import mimir.Database
import mimir.algebra._
import org.apache.spark.sql.catalyst.plans.logical.Statistics

object GetSparkStatistics
{
  def apply(db:Database, query:Operator): Statistics =
  {
    db.raToSpark
      .mimirOpToSparkOp(query)
      .stats
  }
}