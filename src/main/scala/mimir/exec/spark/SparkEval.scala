package mimir.exec.spark

import org.apache.spark.sql.catalyst.expressions.{ Expression, GenericInternalRow }
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReference

import mimir.algebra.{ ID, Type, PrimitiveValue }


object SparkEval 
{

  def bind(e: Expression, schema: Seq[(ID, Type)]): Expression =
  {
    bindReference(
      e, 
      RAToSpark.mimirSchemaToAttributeSeq(schema)
    )
  }

  def eval(e: Expression, tuple: Seq[PrimitiveValue]): PrimitiveValue =
  {
    val row = RAToSpark.mimirPrimitivesToSparkInternalRow(tuple)
    RAToSpark.sparkInternalRowValueToMimirPrimitive(
      e.eval(row)
    )
  }

  def eval(elist: Seq[Expression], tuple: Seq[PrimitiveValue]): Seq[PrimitiveValue] =
  {
    val row = RAToSpark.mimirPrimitivesToSparkInternalRow(tuple)
    elist.map { e =>
      RAToSpark.sparkInternalRowValueToMimirPrimitive(
        e.eval(row)
      )
    }
  }
}