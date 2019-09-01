package mimir.nullables

import mimir.Database
import mimir.algebra._

object NullifyInvalidCells extends InvalidValueHandler
{
  def train(
    db: Database,
    name: ID,
    column: ID,
    rule: Expression,
    query: Operator
  )
  {}

  def view(
    db: Database,
    name: ID,
    column: ID,
    query: Operator,
    rule: Expression,
    friendlyName: String
  ): Operator =
  {
    val message = 
      rule match {
        case Not(IsNullExpression(e)) => 
          StringPrimitive(s"$friendlyName.$column was unexpectedly NULL")
        case _ => 
          Function(ID("concat"), Seq(
            InvalidValueHandler.ruleFailedMessage(friendlyName, column, rule), 
            StringPrimitive(s", so replaced it with NULL")
          ))
      }
    query.alterColumnsByID(
      column -> rule.thenElse { Var(column) }
                              { Caveat(name, NullPrimitive(), Seq(RowIdVar()), message) }
    )
  }
}