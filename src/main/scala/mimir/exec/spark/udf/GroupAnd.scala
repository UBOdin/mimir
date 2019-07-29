package mimir.exec.spark.udf

import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types.{ DataType, BooleanType }
import org.apache.spark.sql.catalyst.expressions.{ AttributeReference, Literal, And }

case class GroupAnd(child: org.apache.spark.sql.catalyst.expressions.Expression) extends DeclarativeAggregate {
  override def children: Seq[org.apache.spark.sql.catalyst.expressions.Expression] = child :: Nil
  override def nullable: Boolean = false
  // Return data type.
  override def dataType: DataType = BooleanType
  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForOrderingExpr(child.dataType, "function group_and")
  private lazy val group_and = AttributeReference("group_and", BooleanType)()
  override lazy val aggBufferAttributes: Seq[AttributeReference] = group_and :: Nil
  override lazy val initialValues: Seq[Literal] = Seq(
    Literal.create(true, BooleanType)
  )
  override lazy val updateExpressions: Seq[ org.apache.spark.sql.catalyst.expressions.Expression] = Seq(
    And(group_and, child)
  )
  override lazy val mergeExpressions: Seq[org.apache.spark.sql.catalyst.expressions.Expression] = {
    Seq(
      And(group_and.left, group_and.right)
    )
  }
  override lazy val evaluateExpression: AttributeReference = group_and
}
