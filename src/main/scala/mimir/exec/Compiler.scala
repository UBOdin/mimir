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
import mimir.exec.mode._
import mimir.exec.uncertainty._
import mimir.util._
import net.sf.jsqlparser.statement.select._
import mimir.gprom.algebra.OperatorTranslation

class Compiler(db: Database) extends LazyLogging {

  val rnd = new Random

  /**
   * Perform a full end-end compilation pass producing best guess results.  
   * Return an iterator over the result set.  
   */
  def compile[R <:ResultIterator](query: Operator, mode: CompileMode[R]): R =
    mode(db, query)

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
      oper = oper.project( requiredColumns.toSeq:_* )

      val (sql, sqlSchema) = sqlForBackend(oper, opts)

      logger.info(s"PROJECTIONS: $projections")

      new ProjectionResultIterator(
        outputCols.map( projections(_) ),
        annotationCols.map( projections(_) ).toSeq,
        sqlSchema,
        new JDBCResultIterator(
          sqlSchema,
          sql, db.backend,
          db.backend.dateType
        )
      )
    }
  }

  def sqlForBackend(
    oper: Operator, 
    opts: Compiler.Optimizations = Compiler.standardOptimizations
  ): 
    (SelectBody, Seq[(String,Type)]) =
  {
    val optimized = { 
      if(ExperimentalOptions.isEnabled("GPROM-OPTIMIZE")
        && db.backend.isInstanceOf[mimir.sql.GProMBackend] ) {
        Compiler.gpromOptimize(oper) 
      } else { 
        Compiler.optimize(oper, opts)
      }
    }
    //val optimized =  Compiler.optimize(oper, opts)

    logger.debug(s"PRE-SPECIALIZED: $oper")

    // The final stage is to apply any database-specific rewrites to adapt
    // the query to the quirks of each specific target database.  Each
    // backend defines a specializeQuery method that handles this
    val specialized = db.backend.specializeQuery(optimized)

    logger.info(s"SPECIALIZED: $specialized")

    logger.info(s"SCHEMA: ${oper.schema.mkString(", ")} -> ${optimized.schema.mkString(", ")}")
    
    // Generate the SQL
    val sql = db.ra.convert(specialized)

    logger.info(s"SQL: $sql")

    return (sql, optimized.schema)
  }
  
  case class VirtualizedQuery(
    query: Operator,
    visibleSchema: Seq[(String, Type)],
    colDeterminism: Map[String, Expression],
    rowDeterminism: Expression,
    provenance: Seq[String]
  )
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
        logger.debug(s"Optimizing, cycle $i: \n$oper")
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
    return oper
  }
  
  def gpromOptimize(rawOper: Operator): Operator = {
    OperatorTranslation.optimizeWithGProM(rawOper)
  }
  
}