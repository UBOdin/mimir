package mimir.exec.spark.udf

import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate

case class GroupBitwiseAnd(child: org.apache.spark.sql.catalyst.expressions.Expression) extends DeclarativeAggregate {
  override def children: Seq[org.apache.spark.sql.catalyst.expressions.Expression] = child :: Nil
  override def nullable: Boolean = false
  // Return data type.
  override def dataType: DataType = LongType
  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForOrderingExpr(child.dataType, "function group_bitwise_and")
  private lazy val group_bitwise_and = AttributeReference("group_bitwise_and", LongType)()
  override lazy val aggBufferAttributes: Seq[AttributeReference] = group_bitwise_and :: Nil
  override lazy val initialValues: Seq[Literal] = Seq(
    Literal.create(0xffffffffffffffffl, LongType)
  )
  override lazy val updateExpressions: Seq[ org.apache.spark.sql.catalyst.expressions.Expression] = Seq(
    BitwiseAnd(group_bitwise_and, child)
  )
  override lazy val mergeExpressions: Seq[org.apache.spark.sql.catalyst.expressions.Expression] = {
    Seq(
      BitwiseAnd(group_bitwise_and.left, group_bitwise_and.right)
    )
  }
  override lazy val evaluateExpression: AttributeReference = group_bitwise_and
}
