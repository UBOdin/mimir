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
import mimir.optimizer.operator._
import mimir.optimizer.expression._
import mimir.provenance._
import mimir.exec.result._
import mimir.exec.mode._
import mimir.exec.uncertainty._
import mimir.util._
import net.sf.jsqlparser.statement.select._
import mimir.gprom.algebra.OperatorTranslation

class Compiler(db: Database) extends LazyLogging {

  def operatorOptimizations: Seq[OperatorOptimization] =
    Seq(
      ProjectRedundantColumns,
      InlineProjections,
      PushdownSelections,
      new PropagateEmptyViews(db.typechecker),
      PropagateConditions,
      new OptimizeExpressions(optimize(_:Expression)),
      PartitionUncertainJoins,
      PullUpUnions
    )

  def expressionOptimizations: Seq[ExpressionOptimizerRule] =
    Seq(
      PullUpBranches,
      new FlattenTrivialBooleanConditionals(db.typechecker),
      // FlattenBooleanConditionals,
      RemoveRedundantCasts,
      PushDownNots,
      new SimplifyExpressions(db.interpreter, db.functions)
    )

  val rnd = new Random

  /**
   * Perform a full end-end compilation pass producing best guess results.  
   * Return an iterator over the result set.  
   */
  def compile[R <:ResultIterator](query: Operator, mode: CompileMode[R]): R =
    mode(db, query)

  def deploy(
    compiledOper: Operator, 
    outputCols: Seq[String]
  ): ResultIterator =
  {
    var oper = compiledOper
    val isAnOutputCol = outputCols.toSet

    // Optimize
    oper = optimize(oper)

    // Run a final typecheck to check the sanitity of the rewrite rules
    val schema = db.typechecker.schemaOf(oper)
    logger.debug(s"SCHEMA: $schema.mkString(", ")")

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
      DecomposeAggregates(oper, db.typechecker) match {
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
      val sourceColumnTypes = db.typechecker.schemaOf(unionClauses(0)).toMap


      val nested = unionClauses.map { deploy(_, requiredColumnsInOrder) }
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
              jointIterator,
              db
            )
        }
      return new ProjectionResultIterator(
        outputCols.map( projections(_) ),
        annotationCols.map( projections(_) ).toSeq,
        db.typechecker.schemaOf(oper),
        aggregateIterator, 
        db
      )

    } else {
      // Make the set of columns we're interested in explicitly part of the query
      oper = oper.project( requiredColumns.toSeq:_* )

      val (sql, sqlSchema) = sqlForBackend(oper)

      logger.info(s"PROJECTIONS: $projections")

      new ProjectionResultIterator(
        outputCols.map( projections(_) ),
        annotationCols.map( projections(_) ).toSeq,
        sqlSchema,
        new JDBCResultIterator(
          sqlSchema,
          sql, db.backend,
          db.backend.dateType
        ),
        db
      )
    }
  }

  def sqlForBackend(
    oper: Operator
  ): 
    (SelectBody, Seq[(String,Type)]) =
  {
    val optimized = { 
      if(ExperimentalOptions.isEnabled("GPROM-OPTIMIZE")
        && db.backend.isInstanceOf[mimir.sql.GProMBackend] ) {
        Compiler.gpromOptimize(oper) 
      } else { 
        optimize(oper)
      }
    }
    //val optimized =  Compiler.optimize(oper, opts)

    logger.debug(s"PRE-SPECIALIZED: $oper")

    // The final stage is to apply any database-specific rewrites to adapt
    // the query to the quirks of each specific target database.  Each
    // backend defines a specializeQuery method that handles this
    val specialized = db.backend.specializeQuery(optimized, db)

    logger.info(s"SPECIALIZED: $specialized")

    logger.info(s"SCHEMA: ${oper.columnNames.mkString(", ")} -> ${optimized.columnNames.mkString(", ")}")
    
    // Generate the SQL
    val sql = db.ra.convert(specialized)

    logger.info(s"SQL: $sql")

    return (sql, db.typechecker.schemaOf(optimized))
  }
  
  // case class VirtualizedQuery(
  //   query: Operator,
  //   visibleSchema: Seq[(String, Type)],
  //   colDeterminism: Map[String, Expression],
  //   rowDeterminism: Expression,
  //   provenance: Seq[String]
  // )

  def optimize(query: Operator) = Optimizer.optimize(query, operatorOptimizations)

  def optimize(e: Expression): Expression = Optimizer.optimize(e, expressionOptimizations)
}

