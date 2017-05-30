package mimir.exec

import mimir.Database
import mimir.optimizer._
import mimir.algebra._
import mimir.ctables._
import mimir.provenance._
import mimir.views._
import mimir.exec.uncertainty.{Statistic, Confidence, ColumnStatistic}
import com.typesafe.scalalogging.slf4j.LazyLogging

class TupleSampler(db: Database, sampleSeeds: Seq[Long] = (0l until 10l).toSeq)
  extends LazyLogging
{

  def apply(rawQuery: Operator, stats: Seq[Statistic]): (Operator, Seq[String]) =
  {
    var query = rawQuery
    val baseSchema = rawQuery.columnNames;

    // Annotate the query with tuple identifiers
    val (withProvenance, provenanceCols) = Provenance.compile(query)
    query = withProvenance

    // Construct a set of samples
    val querySamples =
      sampleSeeds.map( compileForWorld(withProvenance, _) )
    val allSamples =
      OperatorUtils.makeUnion( querySamples )

    // Next, we figure out how to compute the specific statistics of interest.
    // There will be two steps in this process: (1) aggregates, (2) postprocess

    // Figure out which Aggregates need to be computed
    val aggregates =
      stats.toSeq.flatMap { 
        case col: ColumnStatistic => col.aggregates
        case Confidence(output) => 
          Some(
            AggFunction("COUNT", false, Seq(), output)
          )
      }

    // Post-process the aggregates with a projection
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

    // Actually assemble the whole thing
    query = 
      Project(
        projections,
        Aggregate(
          provenanceCols.map( Var(_) ),
          aggregates, 
          allSamples
        )
      )


    // Finally Resolve views
    query = db.views.resolve(query)

    // And return
    return (query, provenanceCols)
  }

  def compileForWorld(query: Operator, seed: Long): Operator =
  {
    (
      query.recurExpressions{ expr:Expression => CTAnalyzer.compileSample(expr, IntPrimitive(seed)) }.
            recur(compileForWorld(_, seed))
    ) match {

      case View(name, subquery, annotations) if CTables.isProbabilistic(subquery) => 
        View(name, subquery, annotations + ViewAnnotation.SAMPLES)

      case rewritten => rewritten
    }
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
