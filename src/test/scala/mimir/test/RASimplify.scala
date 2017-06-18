package mimir.test

import mimir.algebra._
import mimir.algebra.function._
import mimir.parser.ExpressionParser
import mimir.optimizer.expression.SimplifyExpressions

trait RASimplify
{
  val functions = new FunctionRegistry
  val interpreter = new Eval(Some(functions))
  val simplifyBase = new SimplifyExpressions(interpreter, functions)

  def simplify(expr: Expression) = simplifyBase(expr)
  def simplify(expr: String) = simplifyBase(ExpressionParser.expr(expr))
}