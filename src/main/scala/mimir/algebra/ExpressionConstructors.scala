package mimir.algebra


trait ExpressionConstructors
{
  def toExpression: Expression

  def eq(other: String): Expression =
    eq(StringPrimitive(other))
  def eq(other: Int): Expression =
    eq(IntPrimitive(other))
  def eq(other: Double): Expression =
    eq(FloatPrimitive(other))
  def eq(other: Expression): Expression =
    Comparison(Cmp.Eq, toExpression, other)
  def eq(other: ID): Expression =
    eq(Var(other))

  def in(other: Seq[Expression]): Expression =
    ExpressionUtils.makeInTest(toExpression, other)

  def neq(other: String): Expression =
    neq(StringPrimitive(other))
  def neq(other: Int): Expression =
    neq(IntPrimitive(other))
  def neq(other: Double): Expression =
    neq(FloatPrimitive(other))
  def neq(other: Expression): Expression =
    Comparison(Cmp.Neq, toExpression, other)

  def gt(other: Long): Expression = gt(IntPrimitive(other))
  def gt(other: Double): Expression = gt(FloatPrimitive(other))
  def gt(other: Expression): Expression =
    Comparison(Cmp.Gt, toExpression, other)

  def gte(other: Long): Expression = gte(IntPrimitive(other))
  def gte(other: Double): Expression = gte(FloatPrimitive(other))
  def gte(other: Expression): Expression =
    Comparison(Cmp.Gte, toExpression, other)

  def lt(other: Long): Expression = lt(IntPrimitive(other))
  def lt(other: Double): Expression = lt(FloatPrimitive(other))
  def lt(other: Expression): Expression =
    Comparison(Cmp.Lt, toExpression, other)

  def lte(other: Long): Expression = lte(IntPrimitive(other))
  def lte(other: Double): Expression = lte(FloatPrimitive(other))
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

  def thenElse(thenClause: Expression)(elseClause: Expression): Expression =
    Conditional(toExpression, thenClause, elseClause)

  def not: Expression =
    ExpressionUtils.makeNot(toExpression)
}