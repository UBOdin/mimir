package mimir.nullables

import mimir.Database
import mimir.algebra._

trait InvalidValueHandler
{
  def train(
    db: Database,
    name: ID,
    column: ID,
    rule: Expression,
    query: Operator
  )

  def view(
    db: Database,
    name: ID,
    column: ID,
    query: Operator,
    rule: Expression,
    friendlyName: String
  ): Operator

}

object InvalidValueHandler
{
  def DEFAULT = ID("NULLIFY")

  def rules = Map[ID, InvalidValueHandler](
    ID("DROP") -> DropInvalidRows,
    ID("NULLIFY") -> NullifyInvalidCells
  )

  def ruleFailedMessage(friendlyName: String, column: ID, rule: Expression): Expression =
  {
    rule match {
      case Not(IsNullExpression(Var(cmp))) if cmp.equals(column) => 
        StringPrimitive(s"$friendlyName.$column is NULL")
      case _ => 
        StringPrimitive(s"Failed assertion $rule on $friendlyName")

    }
  }
}