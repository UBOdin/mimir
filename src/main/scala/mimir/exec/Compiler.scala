package mimir.exec


import java.sql._
import com.typesafe.scalalogging.slf4j.LazyLogging
import scala.util.Random

import mimir.Database
import mimir.algebra.Union
import mimir.algebra._
import mimir.ctables._
import mimir.optimizer._
import mimir.provenance._
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
    
    val compiled = BestGuesser(db, oper, opts)
    oper               = compiled._1
    val outputSchema   = compiled._2
    val colDeterminism = compiled._3
    val rowDeterminism = compiled._4
    val provenanceCols = compiled._5

    // The deterministic result set iterator should strip off the 
    // provenance columns.  Figure out which columns need to be
    // kept.  Note that the order here actually matters.
    val tagPlusOutputSchemaNames = 
      outputSchema.map(_._1).toList ++
        colDeterminism.toList.flatMap( x => ExpressionUtils.getColumns(x._2)) ++ 
        ExpressionUtils.getColumns(rowDeterminism)

    // We'll need it a few times, so cache the final operator's schema.
    // This also forces the typechecker to run, so we get a final sanity
    // check on the output of the rewrite rules.
    val finalSchema = oper.schema

    // We'll need to line the attributes in the output up with
    // the order in which the user expects to see them.  Build
    // a lookup table with name + position in the query being execed.
    val finalSchemaOrderLookup = 
      finalSchema.map(_._1).zipWithIndex.toMap
   
    logger.debug(s"SCHEMA: $finalSchema")

    val sql = sqlForBackend(oper, opts)

    // Deploy to the backend
    val results = 
      db.backend.execute(sql)

    // And wrap the results.
    new NonDetIterator(
      new ResultSetIterator(results, 
        finalSchema.toMap,
        tagPlusOutputSchemaNames.map(finalSchemaOrderLookup(_)), 
        provenanceCols.map(finalSchemaOrderLookup(_))
      ),
      outputSchema,
      outputSchema.map(_._1).map(colDeterminism(_)), 
      rowDeterminism
    )
  }

  def compileForSamples(
    rawOper: Operator, 
    opts: Compiler.Optimizations = Compiler.standardOptimizations, 
    seeds: Seq[Long] = (0 until 10).map { _ => rnd.nextLong() }
  ): SampleResultIterator =
  {
    var oper = rawOper
    logger.debug(s"COMPILING FOR SAMPLES: $oper")
    
    oper = Compiler.optimize(oper, opts);
    logger.debug(s"OPTIMIZED: $oper")

    val bundled = TupleBundler(db, oper, seeds)
    oper               = bundled._1
    val nonDetColumns  = bundled._2
    val provenanceCols = bundled._3
    val provenanceColSet = provenanceCols.toSet

    logger.debug(s"BUNDLED: $oper")

    oper = Compiler.optimize(oper, opts);

    logger.debug(s"RE-OPTIMIZED: $oper")

    // We'll need it a few times, so cache the final operator's schema.
    // This also forces the typechecker to run, so we get a final sanity
    // check on the output of the rewrite rules.
    val finalSchema = oper.schema

    // We'll need to line the attributes in the output up with
    // the order in which the user expects to see them.  Build
    // a lookup table with name + position in the query being execed.
    val finalSchemaOrderLookup = 
      finalSchema.map(_._1).zipWithIndex.toMap

    val nonProvenanceSchema = 
      finalSchema.
        map(_._1).
        filter { col => !provenanceColSet(col) }

    val sql = sqlForBackend(oper, opts)

    // Deploy to the backend
    val results = 
      db.backend.execute(sql)

    new SampleResultIterator(
      new ResultSetIterator(results, 
        finalSchema.toMap,
        nonProvenanceSchema.map(finalSchemaOrderLookup(_)), 
        provenanceCols.map(finalSchemaOrderLookup(_))
      ),
      rawOper.schema,
      nonDetColumns,
      seeds.size
    )
  }

  def sqlForBackend(oper: Operator, opts: Compiler.Optimizations = Compiler.standardOptimizations): SelectBody =
  {
    val optimized = Compiler.optimize(oper)

    // The final stage is to apply any database-specific rewrites to adapt
    // the query to the quirks of each specific target database.  Each
    // backend defines a specializeQuery method that handles this
    val specialized = db.backend.specializeQuery(optimized)

    logger.debug(s"SPECIALIZED: $specialized")
    
    // Generate the SQL
    val sql = db.ra.convert(specialized)

    logger.debug(s"SQL: $sql")

    return sql
  }

}

object Compiler
  extends LazyLogging
{

  type Optimization = Operator => Operator
  type Optimizations = Seq[Optimization]

  def standardOptimizations: Optimizations =
    Seq(
      ProjectRedundantColumns(_),
      InlineProjections(_),
      PushdownSelections(_),
      PropagateEmptyViews(_)
    )

  /**
   * Optimize the query
   */
  def optimize(rawOper: Operator, opts: Optimizations = standardOptimizations): Operator = {
    var oper = rawOper
    // Repeatedly apply optimizations up to a fixed point or an arbitrary cutoff of 10 iterations
    for( i <- (0 until 10) ){
      logger.debug(s"Optimizing, cycle $i: \n$oper")
      // Try to optimize
      val newOper = 
        opts.foldLeft(oper)((o, fn) => fn(o))

      // Return if we've reached a fixed point 
      if(oper.equals(newOper)){ return newOper; }

      // Otherwise repeat
      oper = newOper;
    }
    return oper;
  }
}