package mimir.exec


import java.sql._
import org.slf4j.{LoggerFactory}
import com.typesafe.scalalogging.slf4j.LazyLogging
import scala.util.Random

import mimir.Database
import mimir.algebra.Union
import mimir.algebra._
import mimir.ctables._
import mimir.optimizer._
import mimir.provenance._
import mimir.exec.stream._
import mimir.util._
import net.sf.jsqlparser.statement.select._
import mimir.gprom.algebra.OperatorTranslation

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

    // Fold the annotations back in
    oper =
      Project(
        rawOper.columnNames.map { name => ProjectArg(name, Var(name)) } ++
        colDeterminism.map { case (name, expression) => ProjectArg(CTPercolator.mimirColDeterministicColumnPrefix + name, expression) } ++
        Seq(
          ProjectArg(CTPercolator.mimirRowDeterministicColumnName, rowDeterminism),
          ProjectArg(Provenance.rowidColnameBase, Function(Provenance.mergeRowIdFunction, provenanceCols.map( Var(_) ) ))
        ),// ++ provenanceCols.map(pc => ProjectArg(pc,Var(pc))),
        oper
      )

    logger.debug(s"FULL STACK: $oper")

    deploy(oper, rawOper.columnNames, opts)

  }

  def compileForSamples(
    rawOper: Operator, 
    opts: Compiler.Optimizations = Compiler.standardOptimizations, 
    seeds: Seq[Long] = (0 until 10).map { _ => rnd.nextLong() }
  ): SampleResultIterator =
  {
    var oper = rawOper
    logger.debug(s"COMPILING FOR SAMPLES: $oper")
    
    oper = Compiler.optimize(oper, Seq(InlineProjections(_)));
    logger.debug(s"OPTIMIZED: $oper")

    val bundled = TupleBundler(db, oper, seeds)
    oper               = bundled._1
    val nonDetColumns  = bundled._2
    val provenanceCols = bundled._3

    logger.debug(s"BUNDLED: $oper")

    oper = Compiler.optimize(oper, opts);

    logger.debug(s"RE-OPTIMIZED: $oper")

    new SampleResultIterator(
      deploy(oper, rawOper.columnNames, opts),
      rawOper.schema,
      nonDetColumns,
      seeds.size
    )
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

    val sql = sqlForBackend(oper, opts)

    // Deploy to the backend
    val results = 
      TimeUtils.monitor("EXECUTE", logger.info(_)){
        db.backend.execute(sql)
      }

    new ProjectionResultIterator(
      outputCols.map( projections(_) ),
      annotationCols.map( projections(_) ).toSeq,
      oper.schema,
      results,
      (db.backend.rowIdType, db.backend.dateType)
    )
  }

  def sqlForBackend(oper: Operator, opts: Compiler.Optimizations = Compiler.standardOptimizations): SelectBody =
  {
    val optimized = { if(db.backend.isInstanceOf[mimir.sql.GProMBackend] ) Compiler.gpromOptimize(oper) else Compiler.optimize(oper, opts)}

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
  
  case class VirtualizedQuery(
    query: Operator,
    visibleSchema: Seq[(String, Type)],
    colDeterminism: Map[String, Expression],
    rowDeterminism: Expression,
    provenance: Seq[String]
  )
  
  /** 
   * The body of the end-end compilation pass.  Return a marked-up version of the
   * query.  This part should be entirely superceded by GProM.
   */
  /*def virtualize(rawOper: Operator, opts: List[Operator => Operator]): VirtualizedQuery =
  {
    // Recursively expand all view tables using mimir.optimizer.ResolveViews
    var oper = ResolveViews(db, rawOper)

    logger.debug(s"RAW: $oper")
    
    // We'll need the pristine pre-manipulation schema down the line
    // As a side effect, this also forces the typechecker to run, 
    // acting as a sanity check on the query before we do any serious
    // work.
    val visibleSchema = oper.schema;

    // The names that the provenance compilation step assigns will
    // be different depending on the structure of the query.  As a 
    // result it is **critical** that this be the first step in 
    // compilation.  
    val provenance = Provenance.compile(oper)
    oper               = provenance._1
    val provenanceCols = provenance._2


    // Tag rows/columns with provenance metadata
    val tagging = CTPercolator.percolateLite(oper)
    oper               = tagging._1
    val colDeterminism = tagging._2
    val rowDeterminism = tagging._3    

    logger.debug(s"PRE-OPTIMIZED: $oper")

    // Clean things up a little... make the query prettier, tighter, and 
    // faster
    oper = optimize(oper, opts)

    logger.debug(s"OPTIMIZED: $oper")

    // Replace VG-Terms with their "Best Guess values"
    oper = bestGuessQuery(oper)

    logger.debug(s"GUESSED: $oper")

    VirtualizedQuery(
      oper, 
      visibleSchema,
      colDeterminism,
      rowDeterminism,
      provenanceCols
    )
  }*/

}

object Compiler
{

  val logger = LoggerFactory.getLogger("mimir.exec.Compiler")

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
    TimeUtils.monitor("OPTIMIZE", logger.info(_)){
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
    }
    return oper
  }
  
  def gpromOptimize(rawOper: Operator): Operator = {
    OperatorTranslation.optimizeWithGProM(rawOper)
  }
  
}