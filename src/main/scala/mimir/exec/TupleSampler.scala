package mimir.exec

import mimir.Database
import mimir.optimizer._
import mimir.algebra._
import mimir.ctables._
import mimir.provenance._
import mimir.exec.stats.{Statistic, Confidence, ColumnStatistic}
import com.typesafe.scalalogging.slf4j.LazyLogging

class TupleSampler(db: Database, sampleSeeds: Seq[Long] = (0l until 10l).toSeq)
  extends LazyLogging
{

  def apply(query: Operator, stats: Seq[Statistic]): (Operator, Seq[String]) =
  {
    val baseSchema = query.columnNames;
    val (withProvenance, provenanceCols) = Provenance.compile(query)

    val querySamples =
      sampleSeeds.map( compileForWorld(withProvenance, _) )

    val allSamples =
      OperatorUtils.makeUnion( querySamples )

    val aggregates =
      stats.toSeq.flatMap { 
        case col: ColumnStatistic => col.aggregates
        case Confidence(output) => 
          Some(
            AggFunction("COUNT", false, Seq(), output)
          )
      }

    val projections =
      stats.toSeq.flatMap {
        case col: ColumnStatistic => col.projections
        case Confidence(output) => 
          Some(
            ProjectArg(output, 
              Arithmetic(Arith.Div, 
                Var(output), 
                FloatPrimitive(sampleSeeds.length)
              )
            )
          )
      }

    (
      Project(
        projections,
        Aggregate(
          provenanceCols.map( Var(_) ),
          aggregates, 
          query
        )
      ),
      provenanceCols  
    )
  }

  def compileForWorld(query: Operator, seed: Long): Operator =
  {
    query.recurExpressions{ expr:Expression => CTAnalyzer.compileSample(expr, IntPrimitive(seed)) }.
          recur(compileForWorld(_, seed))
  }
}

object TupleSampler
{
  def apply(
    db: Database, 
    query: Operator, 
    stats: Seq[Statistic],
    sampleSeeds: Seq[Long] = (0l until 10l).toSeq
  ): (Operator, Seq[String]) =
  {
    new TupleSampler(db, sampleSeeds)(query, stats)
  }
}
