package mimir.exec.mode

import mimir.Database
import mimir.optimizer._
import mimir.algebra._
import mimir.ctables._
import mimir.provenance._
import mimir.views._
import mimir.models.Model
import mimir.exec.result.ResultIterator
import mimir.exec.uncertainty.{Statistic, Confidence, ColumnStatistic}
import com.typesafe.scalalogging.slf4j.LazyLogging


class StatsQuery(
  val stats: Seq[SampleRows.StatArg],
  source: Operator
) extends Project(stats.map { case (alias, stat, args) =>
    ProjectArg(alias, Function("STAT_"+stat.toString, args))
  }, source)

class SampleRows(
  sampleSeeds: Seq[Long] = (0l until 10l).toSeq
)
  extends CompileMode[ResultIterator]
  with LazyLogging
{
  type MetadataT = Seq[String]

  def rewrite(db: Database, rawQuery: Operator): (Operator, Seq[String], Seq[String]) =
  {
    var query = rawQuery

    val stats: Seq[SampleRows.StatArg] = 
      query match {
        case statsQuery: StatsQuery => {
          query = statsQuery.source; 
          statsQuery.stats
        }
        case _ => {
          SampleRows.defaultStats(db.typechecker.schemaOf(query))
        }
      }

    val baseSchema = query.columnNames;

    // Annotate the query with tuple identifiers
    val (withProvenance, provenanceCols) = Provenance.compile(query)
    query = withProvenance

    // Construct a set of samples
    val querySamples =
      sampleSeeds.map( compileForWorld(withProvenance, _, db.models.get(_)) )
    val allSamples =
      OperatorUtils.makeUnion( querySamples )

    // Next, we figure out how to compute the specific statistics of interest.
    // There will be two steps in this process: (1) aggregates, (2) postprocess

    // Figure out which Aggregates need to be computed
    val aggregates =
      stats.flatMap { SampleRows.aggFunctionsFor(_, sampleSeeds.size) }

    // Post-process the aggregates with a projection
    val projections =
      stats.flatMap { SampleRows.projectionsFor(_, sampleSeeds.size) }

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
    return (query, baseSchema, provenanceCols)
  }

  def compileForWorld(query: Operator, seed: Long, models:(String => Model)): Operator =
  {
    (
      query.recurExpressions{ expr:Expression => CTAnalyzer.compileSample(expr, IntPrimitive(seed), models) }.
            recur(compileForWorld(_, seed, models))
    ) match {

      case View(name, subquery, annotations) if CTables.isProbabilistic(subquery) => 
        View(name, subquery, annotations + ViewAnnotation.SAMPLES)

      case rewritten => rewritten
    }
  }

  def wrap(db: Database, results: ResultIterator, query: Operator, meta: Seq[String]): ResultIterator =
    results
}


object SampleRows
{
  object Stat extends Enumeration
  {
    type T = Value
    val EXPECTATION, STDDEV, ANYVALUE, CONFIDENCE = Value
  }

  type StatArg = (String, Stat.T, Seq[Expression])

  def defaultStats(schema: Seq[(String, Type)]): Seq[StatArg] =
  {
    schema.flatMap { 
      case (name, (TInt() | TFloat())) => 
        Seq(
          (name,           Stat.EXPECTATION, Seq(Var(name))),
          (name+"-STDDEV", Stat.STDDEV,      Seq(Var(name)))
        )
      case (name, _) =>
        Seq(
          (name,           Stat.ANYVALUE,    Seq(Var(name)))
        )
    } ++ Seq(
          ("CONFIDENCE",   Stat.CONFIDENCE,  Seq())
    )
  }

  def aggFunctionsFor(fn: StatArg, numSamples: Int): Seq[AggFunction] =
  {
    val (alias, stat, args) = fn
    (stat, args) match {
      case (Stat.EXPECTATION, args) => Seq(AggFunction("AVG",    false, args,  alias))
      case (Stat.STDDEV,      args) => Seq(AggFunction("STDDEV", false, args,  alias))
      case (Stat.ANYVALUE,    args) => Seq(AggFunction("FIRST",  false, args,  alias))
      case (Stat.CONFIDENCE,  _   ) => Seq(AggFunction("COUNT",  false, Seq(), alias))
    }
  }

  def projectionsFor(fn: StatArg, numSamples: Int): Seq[ProjectArg] =
  {
    val (alias, stat, args) = fn
    (stat, args) match {
      case (Stat.EXPECTATION, args) => Seq(ProjectArg(alias, Var(alias)))
      case (Stat.STDDEV,      args) => Seq(ProjectArg(alias, Var(alias)))
      case (Stat.ANYVALUE,    args) => Seq(ProjectArg(alias, Var(alias)))
      case (Stat.CONFIDENCE,  _   ) => Seq(ProjectArg(alias, Var(alias).div(FloatPrimitive(numSamples))))
    }
  }
}