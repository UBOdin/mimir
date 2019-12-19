package mimir.exec.shortcut

import scala.collection.mutable.{ 
  Map => MutableMap, 
  Set => MutableSet
}

import org.apache.spark.sql.catalyst.expressions.{ 
  GenericInternalRow, 
  Attribute,
  Expression => SparkExpression
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

import mimir.Database
import mimir.algebra._
import mimir.exec.result.ResultIterator
import mimir.exec.spark.{ MimirSpark, RAToSpark }

/**
 * Base class for aggregate state objects
 */
sealed trait AggregateEval
{
  def update(tuple: Seq[PrimitiveValue])
  def finish(): PrimitiveValue
}


/** 
 * Utility code for inline execution of aggregates
 */
object ComputeAggregate
{
  def apply(
    groupby: Seq[ID], 
    aggregates: Seq[AggFunction], 
    input: ResultIterator,
    db: Database
  ): Seq[Seq[PrimitiveValue]] =
  {
    val groups = MutableMap[Seq[PrimitiveValue], Seq[AggregateEval]]()
    val mkGroup, inputExprs = 
      aggregates.map { compile(_, input, db) }
                .unzip
    ???
  }

  def compile(agg: AggFunction, input: ResultIterator, db: Database): 
    ( 
      () => AggregateEval,
      Seq[(Attribute, Expression)]
    ) =
  {
    val schemaMap = input.tupleSchema.toMap
    val fields:Seq[(Attribute, Expression)] = 
      agg.args.zipWithIndex.map { case (expr, idx) => 
        val fieldName = ID(s"${agg.alias.id}:$idx")
        val fieldType = db.typechecker.typeOf(expr, schemaMap(_))

        val attr = RAToSpark.mimirColumnToAttribute(fieldName -> fieldType)

        (attr -> expr)
      }

    val aggDefn:AggregateFunction = 
      MimirSpark.makeAggregate(agg.function, fields.map { _._1 })

    val aggregateEval = 
      aggDefn match { 
        case decl:DeclarativeAggregate => 
        {
          val emptyRow = new GenericInternalRow(Array[Any]())
          val schema = fields.map { _._1 } ++ decl.aggBufferAttributes
          val updateExpressions = 
            decl.updateExpressions.map { bindReference(_, schema) }
          val finalExpression =
            bindReference(decl.evaluateExpression, decl.aggBufferAttributes)
          val initialValues = decl.initialValues.map { _.eval(emptyRow) }

          { () => new SparkDeclarativeAggregate(initialValues, updateExpressions, finalExpression) }
        }
        case imp:ImperativeAggregate => 
        {
          { () => new SparkImperativeAggregate(imp) }
        }
      }

    if(agg.distinct){
      return (
        { () => new DistinctAggregate(aggregateEval(), fields) },
        fields
      )
    } else {
      return (aggregateEval, fields)
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

  def update(tuple: Seq[PrimitiveValue]) = 
  {

    val record = new GenericInternalRow( (
      tuple.map { RAToSpark.mimirPrimitiveToSparkInternalRowValue(_) }
        ++ buffer).toArray )
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

  def update(tuple: Seq[PrimitiveValue]) = 
  {
    val inputRecord = new GenericInternalRow(
      tuple.map { RAToSpark.mimirPrimitiveToSparkInternalRowValue(_) }
           .toArray
    )
    agg.update(buffer, inputRecord)
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
  agg: AggregateEval
)
  extends AggregateEval
{

  val buffer = MutableSet[Seq[PrimitiveValue]]()

  def update(tuple: Seq[PrimitiveValue])
  {
    buffer.add(tuple)
  }
  def finish(): PrimitiveValue =
  {
    for(row <- buffer){ agg.update(row) }
    return agg.finish()
  }
}
