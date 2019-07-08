package mimir.exec.spark.udf

import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate

case class GroupBitwiseOr(child: org.apache.spark.sql.catalyst.expressions.Expression) extends DeclarativeAggregate {
  override def children: Seq[org.apache.spark.sql.catalyst.expressions.Expression] = child :: Nil
  override def nullable: Boolean = false
  // Return data type.
  override def dataType: DataType = LongType
  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForOrderingExpr(child.dataType, "function group_bitwise_or")
  private lazy val group_bitwise_or = AttributeReference("group_bitwise_or", LongType)()
  override lazy val aggBufferAttributes: Seq[AttributeReference] = group_bitwise_or :: Nil
  override lazy val initialValues: Seq[Literal] = Seq(
    Literal.create(0, LongType)
  )
  override lazy val updateExpressions: Seq[ org.apache.spark.sql.catalyst.expressions.Expression] = Seq(
    BitwiseOr(group_bitwise_or, child)
  )
  override lazy val mergeExpressions: Seq[org.apache.spark.sql.catalyst.expressions.Expression] = {
    Seq(
      BitwiseOr(group_bitwise_or.left, group_bitwise_or.right)
    )
  }
  override lazy val evaluateExpression: AttributeReference = group_bitwise_or
}
