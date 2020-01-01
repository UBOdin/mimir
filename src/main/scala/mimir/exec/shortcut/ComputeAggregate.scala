package mimir.exec.shortcut


import scala.collection.mutable.{ 
  Map => MutableMap, 
  Set => MutableSet
}

import org.apache.spark.sql.catalyst.analysis.TypeCoercion.ImplicitTypeCasts
import org.apache.spark.sql.catalyst.expressions.{ 
  JoinedRow,
  GenericInternalRow, 
  Attribute,
  Expression => SparkExpression,
  Literal => SparkLiteral,
  ImplicitCastInputTypes
}
import org.apache.spark.sql.catalyst.expressions.aggregate.{
  DeclarativeAggregate,
  ImperativeAggregate,
  AggregateFunction
}
import org.apache.spark.sql.catalog.{
  Function => SparkFunction
}
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReference

import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir.Database
import mimir.algebra._
import mimir.exec.result.ResultIterator
import mimir.exec.spark.{ MimirSpark, RAToSpark }

/**
 * Base class for aggregate state objects
 */
sealed trait AggregateEval
{
  def update(tuple: GenericInternalRow)
  def finish(): PrimitiveValue
}


/** 
 * Utility code for inline execution of aggregates
 */
object ComputeAggregate
  extends LazyLogging
{
  def apply(
    groupby: Seq[ID], 
    aggregates: Seq[AggFunction], 
    input: ResultIterator,
    db: Database
  ): Seq[Seq[PrimitiveValue]] =
  {
    val groups = MutableMap[Seq[PrimitiveValue], Seq[AggregateEval]]()
    val mkGroup = aggregates.map { compile(_, input, db) }
    ???
  }

  def compile(agg: AggFunction, input: ResultIterator, db: Database): 
    ( 
      () => AggregateEval
    ) =
  {
    val inputSchema = RAToSpark.mimirSchemaToAttributeSeq(input.tupleSchema)

    val fields:Seq[SparkExpression] = 
      agg.args
         .map { expr:Expression => 
                db.raToSpark.mimirExprToSparkExpr(input.tupleSchema, expr)
              }
         .map { expr:SparkExpression =>
                db.raToSpark.bindSparkExpressionToTuple(inputSchema, expr)
              }

    val aggDefn:AggregateFunction = 
      MimirSpark.makeAggregate(agg.function, fields)


    val aggregateEval = 
      castAggregateInputs(aggDefn) match { 
        case decl:DeclarativeAggregate => 
        {
          val updateSchema = inputSchema ++ decl.aggBufferAttributes
          val emptyRow = new GenericInternalRow(Array[Any]())
          logger.debug(s"[Before] Initial Values: ${decl.initialValues}")
          logger.debug(s"[Before] Update Expressions: ${decl.updateExpressions}")
          logger.debug(s"[Before] Final Expressions: ${decl.evaluateExpression}")
          val updateExpressions = 
            decl.updateExpressions.map { bindReference(_, updateSchema) }
          val finalExpression =
            bindReference(decl.evaluateExpression, decl.aggBufferAttributes)
          val initialValues = decl.initialValues.map { _.eval(emptyRow) }
          logger.debug(s"[After] Initial Values: $initialValues")
          logger.debug(s"[After] Update Expressions: $updateExpressions")
          logger.debug(s"[After] Final Expressions: $finalExpression")

          { () => new SparkDeclarativeAggregate(initialValues, updateExpressions, finalExpression) }
        }
        case imp:ImperativeAggregate => 
        {
          { () => new SparkImperativeAggregate(imp) }
        }
      }

    if(agg.distinct){
      return { () => new DistinctAggregate(aggregateEval(), fields) }
    } else {
      return aggregateEval
    }

  }

  // Borrowed from Spark's TypeCoersion module.  Ugh... stupid lack of proper abstraction.
  def castAggregateInputs(agg:AggregateFunction): AggregateFunction =
  {
    agg match {
      case e: AggregateFunction with ImplicitCastInputTypes if e.inputTypes.nonEmpty => {
        val children: Seq[SparkExpression] = 
          e.children.zip(e.inputTypes).map { case (in, expected) =>
            ImplicitTypeCasts.implicitCast(in, expected).getOrElse(in)
          }
        e.withNewChildren(children)
         .asInstanceOf[AggregateFunction]
      }
      case _ => agg
    }
  }
}





/**
 * Local execution wrapper for Spark's Declarative Aggregate Function
 */
class SparkDeclarativeAggregate(
  initialValues: Seq[Any],
  updateExpressions: Seq[SparkExpression],
  finishExpression: SparkExpression
) 
  extends AggregateEval
{
  var buffer = initialValues

  def update(tuple: GenericInternalRow) = 
  {

    val record = new JoinedRow(
      tuple, 
      new GenericInternalRow( buffer.toArray )
    )
    buffer = updateExpressions.map { _.eval(record) }
  }
  def finish(): PrimitiveValue = 
  {
    val record = new GenericInternalRow( buffer.toArray )
    RAToSpark.sparkInternalRowValueToMimirPrimitive(
      finishExpression.eval(record)
    )
  }
}

/**
 * Local execution wrapper for Spark's Imperative Aggregate Function
 */
class SparkImperativeAggregate(
  agg: ImperativeAggregate
) 
  extends AggregateEval
{
  val buffer = {
    val row = new GenericInternalRow(
      agg.aggBufferAttributes.map { _ => null }.toArray[Any]
    )
    agg.initialize(row)
    row
  }

  def update(tuple: GenericInternalRow) = 
  {
    agg.update(buffer, tuple)
  }
  def finish(): PrimitiveValue = 
  {
    RAToSpark.sparkInternalRowValueToMimirPrimitive(
      agg.eval(buffer)
    )
  }
}

/**
 * Utility wrapper to allow deduplicated aggregates
 */
class DistinctAggregate(
  agg: AggregateEval,
  key: Seq[SparkExpression]
)
  extends AggregateEval
  with LazyLogging
{
  val buffer = MutableMap[GenericInternalRow, GenericInternalRow]()

  def update(tuple: GenericInternalRow)
  {
    val group = new GenericInternalRow(
      key.map { _.eval(tuple) }
         .toArray
    )
    logger.debug(s"Distinct Aggregate Add: $tuple -> $group")
    buffer.put(group,tuple)
  }
  def finish(): PrimitiveValue =
  {
    for( (_, row) <- buffer){ 
      logger.debug(s"Distinct Aggregate Process: $row")
      agg.update(row) 
    }
    return agg.finish()
  }
}
