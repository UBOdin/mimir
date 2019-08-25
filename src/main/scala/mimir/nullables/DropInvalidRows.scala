package mimir.nullables

import mimir.Database
import mimir.algebra.{ID, Operator, Expression}

object DropInvalidRows extends InvalidValueHandler
{
  def train(
    db: Database,
    name: ID,
    column: ID,
    rule: Expression,
    query: Operator
  )
  {
    ???
  }

  def view(
    db: Database,
    name: ID,
    column: ID,
    query: Operator,
    rule: Expression,
    friendlyName: String
  ): Operator =
  {
    ???
  }
}