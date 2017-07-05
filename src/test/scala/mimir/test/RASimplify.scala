package mimir.test

import mimir.algebra._
import mimir.algebra.function._
import mimir.parser.ExpressionParser
import mimir.optimizer.operator.OptimizeExpressions
import mimir.optimizer.expression.SimplifyExpressions

trait RASimplify
{
  val functions = new FunctionRegistry
  val interpreter = new Eval(Some(functions))
  val simplifyBase = new SimplifyExpressions(interpreter, functions)
  val simplifyOperator = new OptimizeExpressions(simplify(_:Expression))

  def simplify(expr: Expression): Expression = simplifyBase(expr)
  def simplify(expr: String): Expression = simplifyBase(ExpressionParser.expr(expr))
  def simplify(oper: Operator): Operator = simplifyOperator(oper)
}