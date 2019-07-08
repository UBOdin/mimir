package mimir.exec.spark.udf

import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate

case class GroupOr(child: org.apache.spark.sql.catalyst.expressions.Expression) extends DeclarativeAggregate {
  override def children: Seq[org.apache.spark.sql.catalyst.expressions.Expression] = child :: Nil
  override def nullable: Boolean = false
  // Return data type.
  override def dataType: DataType = BooleanType
  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForOrderingExpr(child.dataType, "function group_or")
  private lazy val group_or = AttributeReference("group_or", BooleanType)()
  override lazy val aggBufferAttributes: Seq[AttributeReference] = group_or :: Nil
  override lazy val initialValues: Seq[Literal] = Seq(
    Literal.create(false, BooleanType)
  )
  override lazy val updateExpressions: Seq[ org.apache.spark.sql.catalyst.expressions.Expression] = Seq(
    Or(group_or, child)
  )
  override lazy val mergeExpressions: Seq[org.apache.spark.sql.catalyst.expressions.Expression] = {
    Seq(
      Or(group_or.left, group_or.right)
    )
  }
  override lazy val evaluateExpression: AttributeReference = group_or
}
