package mimir.exec.spark.udf

import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate

case class JsonGroupArray(child: org.apache.spark.sql.catalyst.expressions.Expression) extends DeclarativeAggregate {
  override def children: Seq[org.apache.spark.sql.catalyst.expressions.Expression] = child :: Nil
  override def nullable: Boolean = false
  // Return data type.
  override def dataType: DataType = StringType
  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForOrderingExpr(child.dataType, "function json_group_array")
  private lazy val json_group_array = AttributeReference("json_group_array", StringType)()
  override lazy val aggBufferAttributes: Seq[AttributeReference] = json_group_array :: Nil
  override lazy val initialValues: Seq[Literal] = Seq(
    Literal.create("", StringType)
  )
  override lazy val updateExpressions: Seq[ org.apache.spark.sql.catalyst.expressions.Expression] = Seq(
    If(IsNull(child),
      Concat(Seq(json_group_array, Literal(","), Literal("null"))),
      Concat(Seq(json_group_array, Literal(","), org.apache.spark.sql.catalyst.expressions.Cast(child,StringType,None)))) 
  )
  override lazy val mergeExpressions: Seq[org.apache.spark.sql.catalyst.expressions.Expression] = {
    Seq(
      Concat(Seq(json_group_array.left, json_group_array.right))
    )
  }
  override lazy val evaluateExpression = Concat(Seq(Literal("["), If(StartsWith(json_group_array,Literal(",")),Substring(json_group_array,Literal(2),Literal(Integer.MAX_VALUE)),json_group_array), Literal("]")))
}