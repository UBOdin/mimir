package mimir.exec


import java.sql._
import org.slf4j.{LoggerFactory}
import com.typesafe.scalalogging.slf4j.LazyLogging
import scala.util.Random
import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.{Dataset, DataFrame, SaveMode}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.encoders.RowEncoder

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
import mimir.exec.spark._
import mimir.util._
import sparsity.select.SelectBody

class Compiler(db: Database) extends LazyLogging {

  def operatorOptimizations: Seq[OperatorOptimization] =
    Seq(
      new EvaluateHardTables(db.typechecker, db.interpreter),
      ProjectRedundantColumns,
      InlineProjections,
      PushdownSelections,
      new PropagateEmptyViews(db.typechecker, db.aggregates),
      PropagateConditions,
      new OptimizeExpressions(optimize(_:Expression)),
      PartitionUncertainJoins,
      new PullUpUnions(db.typechecker)
    )

  def expressionOptimizations: Seq[ExpressionOptimizerRule] =
    Seq(
      PullUpBranches,
      PushDownConditionalConstraints,
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
  def compile[R <:ResultIterator](query: Operator, mode: CompileMode[R], rootIteratorGen:(Operator)=>(Seq[(ID,Type)],ResultIterator) ): R =
    mode(db, query, rootIteratorGen)

  def deploy(
    compiledOper: Operator, 
    outputCols: Seq[ID],
    rootIteratorGen:(Operator)=>(Seq[(ID,Type)],ResultIterator)
  ): ResultIterator =
  {
    var oper = compiledOper
    val isAnOutputCol = outputCols.toSet

    // Optimize
    oper = optimize(oper)

    // Run a final typecheck to check the sanitity of the rewrite rules
    val schema = db.typechecker.schemaOf(oper)
    logger.debug(s"SCHEMA: ${schema.mkString(", ")}")

    val (rootIteratorSchema, rootIterator) = 
      rootIteratorGen( oper )
    
    val annotationCols =
      oper.columnNames
          .filter { !isAnOutputCol(_) }
    // Use a projection result iterator to hide the annotation columns
    return new ProjectionResultIterator(
      outputCols.map { col => ProjectArg(col, Var(col)) },
      annotationCols.map { col => ProjectArg(col, Var(col)) },
      rootIteratorSchema, 
      rootIterator,
      db
    )

  }
  
  def sparkBackendRootIterator(oper:Operator) : (Seq[(ID, Type)], ResultIterator) = {
    val schema = db.typechecker.schemaOf(oper) 
    (schema, new SparkResultIterator(schema, oper, db))
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

  def compileToSparkWithoutRewrites(compiledOp: Operator): DataFrame = 
  {
    var sparkOper:LogicalPlan = null
    try {
      logger.trace(s"------------------------ mimir op --------------------------\n$compiledOp")
      logger.trace("------------------------------------------------------------")
      sparkOper = db.raToSpark.mimirOpToSparkOp(compiledOp)
      logger.trace(s"------------------------ spark op --------------------------\n$sparkOper")
      logger.trace("------------------------------------------------------------")
      val qe = MimirSpark.get.sparkSession.sessionState.executePlan(sparkOper)
      qe.assertAnalyzed()
      new Dataset[org.apache.spark.sql.Row](MimirSpark.get, qe.optimizedPlan, RowEncoder(qe.analyzed.schema))
    } catch {
      case t: Throwable => {
        logger.error("-------------------------> Exception Executing Spark Op: " + t.toString() + "\n" + t.getStackTrace.mkString("\n"))
        logger.error(s"------------------------ spark op --------------------------\n$sparkOper")
        logger.error("------------------------------------------------------------")
        throw t
      }
    }
  }

  def compileToSparkWithRewrites(op: Operator): DataFrame =
  {
    val (compiledOp, outputCols, metadata) = 
      mimir.exec.mode.UnannotatedBestGuess.rewrite(db, 
        db.views.rebuildAdaptiveViews(op)
      )
    val optimizedOp = mimir.optimizer.Optimizer.optimize(
      compiledOp.project(outputCols.map(_.id):_*), 
      operatorOptimizations
    ) 
    compileToSparkWithoutRewrites(optimizedOp)
  }
  
  def executeOnWorkers(compiledOp:Operator, dfRowFunc:(Iterator[org.apache.spark.sql.Row]) => Unit):Unit = {
    val df = compileToSparkWithoutRewrites(compiledOp)
    df.foreachPartition(dfRowFunc)                                                                                                  
  }
  
  def mapDatasetToNew(compiledOp:Operator, newDSName:String, mapFunc:(Iterator[org.apache.spark.sql.Row]) => Iterator[org.apache.spark.sql.Row], encoder:org.apache.spark.sql.Encoder[org.apache.spark.sql.Row]): Unit  = {
    import org.apache.spark.sql.functions.sum
    val df = compileToSparkWithoutRewrites(compiledOp)
    val newDF = df.mapPartitions(mapFunc)(encoder).toDF()
    newDF.persist().createOrReplaceTempView(newDSName) 
    newDF.write.mode(SaveMode.ErrorIfExists).saveAsTable(newDSName)
  }   
}



