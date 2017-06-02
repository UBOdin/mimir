package mimir.exec


import java.sql._
import org.slf4j.{LoggerFactory}
import com.typesafe.scalalogging.slf4j.LazyLogging
import scala.util.Random
import com.github.nscala_time.time.Imports._

import mimir.Database
import mimir.algebra.Union
import mimir.algebra._
import mimir.ctables._
import mimir.optimizer._
import mimir.provenance._
import mimir.exec.result._
import mimir.exec.uncertainty._
import mimir.util._
import net.sf.jsqlparser.statement.select._

class Compiler(db: Database) extends LazyLogging {

  val rnd = new Random

  /**
   * Perform a full end-end compilation pass.  Return an iterator over
   * the result set.  
   */
  def compileForBestGuess(rawOper: Operator, opts: Compiler.Optimizations = Compiler.standardOptimizations): ResultIterator = 
  {
    var oper = rawOper
    logger.debug(s"RAW: $oper")
    
    // Compute the best-guess expression
    val compiled = BestGuesser(db, oper, opts)
    oper               = compiled._1
    val outputSchema   = compiled._2
    val colDeterminism = compiled._3
    val rowDeterminism = compiled._4
    val provenanceCols = compiled._5

    logger.trace(s"GUESSED: $oper")

    // Fold the annotations back in
    oper =
      Project(
        rawOper.columnNames.map { name => ProjectArg(name, Var(name)) } ++
        colDeterminism.map { case (name, expression) => ProjectArg(CTPercolator.mimirColDeterministicColumnPrefix + name, expression) } ++
        Seq(
          ProjectArg(CTPercolator.mimirRowDeterministicColumnName, rowDeterminism),
          ProjectArg(Provenance.rowidColnameBase, Function(Provenance.mergeRowIdFunction, provenanceCols.map( Var(_) ) ))
        ),
        oper
      )

    logger.trace(s"FULL STACK: $oper")

    deploy(oper, rawOper.columnNames, opts)

  }

  def compileForSamples(
    rawOper: Operator, 
    opts: Compiler.Optimizations = Compiler.standardOptimizations, 
    seeds: Seq[Long] = (0 until 10).map { _ => rnd.nextLong() }
  ): SampleResultIterator =
  {
    var oper = rawOper
    logger.trace(s"COMPILING FOR SAMPLES: $oper")
    
    oper = Compiler.optimize(oper, Seq(InlineProjections));
    logger.trace(s"OPTIMIZED: $oper")

    val bundled = TupleBundler(db, oper, seeds)
    oper               = bundled._1
    val nonDetColumns  = bundled._2
    val provenanceCols = bundled._3

    logger.trace(s"BUNDLED: $oper")

    oper = Compiler.optimize(oper, opts)

    logger.trace(s"RE-OPTIMIZED: $oper")

    new SampleResultIterator(
      deploy(oper, TupleBundler.splitColumnNames(rawOper.columnNames, nonDetColumns, seeds.length), opts),
      rawOper.schema,
      nonDetColumns,
      seeds.size
    )
  }

  def compileForStats(
    rawOper: Operator,
    stats: Seq[Statistic],
    opts: Compiler.Optimizations = Compiler.standardOptimizations,
    seeds: Seq[Long] = (0 until 10).map { _ => rnd.nextLong() }
  ): ResultIterator =
  {
    var oper = rawOper
    logger.trace(s"COMPILING FOR STATS: $oper")
    
    oper = Compiler.optimize(oper, Seq(InlineProjections));
    logger.trace(s"OPTIMIZED: $oper")

    val sampled = TupleSampler(db, oper, stats, seeds)
    oper               = sampled._1
    val provenanceCols = sampled._2

    logger.trace(s"SAMPLED: $oper")

    oper = Compiler.optimize(oper, opts);

    logger.trace(s"RE-OPTIMIZED: $oper")

    deploy(oper, oper.columnNames, opts)
  }

  def compilePartition(
    rawOper: Operator,
    opts: Compiler.Optimizations = Compiler.standardOptimizations
  ): ResultIterator =
  {
    var oper = rawOper
    logger.trace(s"COMPILING FOR PARTITION: $oper")

    oper = StripViews(oper, onlyProbabilistic = true)

    oper = Compiler.optimize(oper, Seq(InlineProjections));

    oper = CTPercolatorClassic.percolate(oper)

    val partitions =
      OperatorUtils.extractUnionClauses(oper).map {
        OperatorUtils.extractProjections(_)
      }.map { 
        case (proj, oper) => 
          assert(CTables.isDeterministic(oper))
          (proj, Compiler.optimize(oper, opts))
      }.iterator.map { 
        case (proj, oper) => 
          deploy(Project(proj, oper), rawOper.columnNames, Seq())
      }

    return new UnionResultIterator(partitions)

  }

  def deploy(
    compiledOper: Operator, 
    outputCols: Seq[String], 
    opts: Compiler.Optimizations = Compiler.standardOptimizations
  ): ResultIterator =
  {
    var oper = compiledOper
    val isAnOutputCol = outputCols.toSet

    // Run a final typecheck to check the sanitity of the rewrite rules
    val schema = oper.schema
    logger.debug(s"SCHEMA: $schema.mkString(", ")")

    // Optimize
    oper = Compiler.optimize(oper, opts)

    // Strip off the final projection operator
    val extracted = OperatorUtils.extractProjections(oper)
    val projections = extracted._1.map { col => (col.name -> col) }.toMap
    oper            = extracted._2

    val annotationCols =
      projections.keys.filter( !isAnOutputCol(_) )

    val requiredColumns = 
      projections.values
        .map(_.expression)
        .flatMap { ExpressionUtils.getColumns(_) }
        .toSet

    val (agg: Option[(Seq[Var], Seq[AggFunction])], unionClauses: Seq[Operator]) = 
      DecomposeAggregates(oper) match {
        case Aggregate(gbCols, aggCols, src) => 
          (Some((gbCols, aggCols)), OperatorUtils.extractUnionClauses(src))
        case _ => 
          (None, OperatorUtils.extractUnionClauses(oper))
      }
      
    if(unionClauses.size > 1 && ExperimentalOptions.isEnabled("AVOID-IN-SITU-UNIONS")){

      val requiredColumnsInOrder = 
        agg match {
          case None => 
            requiredColumns.toSeq
          case Some((gbCols, aggFunctions)) => 
            gbCols.map { _.name } ++ 
            aggFunctions
              .flatMap { _.args }
              .flatMap { ExpressionUtils.getColumns(_) }
              .toSet.toSeq
        }
      val sourceColumnTypes = unionClauses(0).schema.toMap


      val nested = unionClauses.map { deploy(_, requiredColumnsInOrder, opts = opts) }
      val jointIterator = new UnionResultIterator(nested.iterator)

      val aggregateIterator =
        agg match {
          case None => 
            jointIterator
          case Some((gbCols, aggFunctions)) => 
            new AggregateResultIterator(
              gbCols, 
              aggFunctions,
              requiredColumnsInOrder.map { col => (col, sourceColumnTypes(col)) },
              jointIterator
            )
        }
      return new ProjectionResultIterator(
        outputCols.map( projections(_) ),
        annotationCols.map( projections(_) ).toSeq,
        oper.schema,
        aggregateIterator
      )

    } else {
      // Make the set of columns we're interested in explicitly part of the query
      oper = 
        OperatorUtils.projectDownToColumns(requiredColumns.toSeq, oper)

      val sql = sqlForBackend(oper, opts)

      logger.info(s"PROJECTIONS: $projections")

      new ProjectionResultIterator(
        outputCols.map( projections(_) ),
        annotationCols.map( projections(_) ).toSeq,
        oper.schema,
        new JDBCResultIterator(
          oper.schema,
          sql, db.backend,
          (db.backend.rowIdType, db.backend.dateType)
        )
      )
    }
  }

  def sqlForBackend(oper: Operator, opts: Compiler.Optimizations = Compiler.standardOptimizations): SelectBody =
  {
    val optimized = Compiler.optimize(oper)

    // The final stage is to apply any database-specific rewrites to adapt
    // the query to the quirks of each specific target database.  Each
    // backend defines a specializeQuery method that handles this
    val specialized = db.backend.specializeQuery(optimized)

    logger.info(s"SPECIALIZED: $specialized")
    
    // Generate the SQL
    val sql = db.ra.convert(specialized)

    logger.info(s"SQL: $sql")

    return sql
  }

}

object Compiler
{

  val logger = LoggerFactory.getLogger("mimir.exec.Compiler")

  type Optimizations = Seq[OperatorOptimization]

  def standardOptimizations: Optimizations =
    Seq(
      ProjectRedundantColumns,
      InlineProjections,
      PushdownSelections,
      PropagateEmptyViews,
      PropagateConditions,
      EvaluateExpressions,
      PartitionUncertainJoins,
      PullUpUnions
    )

  /**
   * Optimize the query
   */
  def optimize(rawOper: Operator, opts: Optimizations = standardOptimizations): Operator = {
    var oper = rawOper
    // Repeatedly apply optimizations up to a fixed point or an arbitrary cutoff of 10 iterations
    var startTime = DateTime.now
    TimeUtils.monitor("OPTIMIZE", logger.info(_)){
      for( i <- (0 until 4) ){
        logger.trace(s"Optimizing, cycle $i: \n$oper")
        // Try to optimize
        val newOper = 
          opts.foldLeft(oper) { (o, fn) =>
            logger.trace(s"Applying: $fn")
            fn(o)
          }
          

        // Return if we've reached a fixed point 
        if(oper.equals(newOper)){ return newOper; }

        // Otherwise repeat
        oper = newOper;

        val timeSoFar = startTime to DateTime.now
        if(timeSoFar.millis > 5000){
          logger.warn(s"""OPTIMIZE TIMEOUT (${timeSoFar.millis} ms)
            ---- ORIGINAL QUERY ----\n$rawOper
            ---- CURRENT QUERY (${i+1} iterations) ----\n$oper
            """)
          return oper;
        }
      }
    }
    return oper;
  }
}