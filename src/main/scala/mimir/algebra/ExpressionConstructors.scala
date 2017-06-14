package mimir.algebra

trait ExpressionConstructors
{
  def toExpression: Expression

  def eq(other: Expression): Expression =
    Comparison(Cmp.Eq, toExpression, other)

  def neq(other: Expression): Expression =
    Comparison(Cmp.Neq, toExpression, other)

  def gt(other: Expression): Expression =
    Comparison(Cmp.Gt, toExpression, other)

  def gte(other: Expression): Expression =
    Comparison(Cmp.Gte, toExpression, other)

  def lt(other: Expression): Expression =
    Comparison(Cmp.Lt, toExpression, other)

  def lte(other: Expression): Expression =
    Comparison(Cmp.Lte, toExpression, other)

  def and(other: Expression): Expression =
    Arithmetic(Arith.And, toExpression, other)

  def or(other: Expression): Expression =
    Arithmetic(Arith.Or, toExpression, other)

  def add(other: Expression): Expression =
    Arithmetic(Arith.Add, toExpression, other)

  def sub(other: Expression): Expression =
    Arithmetic(Arith.Sub, toExpression, other)

  def mult(other: Expression): Expression =
    Arithmetic(Arith.Mult, toExpression, other)

  def div(other: Expression): Expression =
    Arithmetic(Arith.Div, toExpression, other)

  def bitAnd(other: Expression): Expression =
    Arithmetic(Arith.BitAnd, toExpression, other)

  def bitOr(other: Expression): Expression =
    Arithmetic(Arith.BitOr, toExpression, other)

  def shiftLeft(other: Expression): Expression =
    Arithmetic(Arith.ShiftLeft, toExpression, other)

  def shiftRight(other: Expression): Expression =
    Arithmetic(Arith.ShiftRight, toExpression, other)

  def isNull: Expression =
    IsNullExpression(toExpression)
}