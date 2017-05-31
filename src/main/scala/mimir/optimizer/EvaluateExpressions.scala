package mimir.optimizer

import mimir.algebra._

object EvaluateExpressions
  extends OperatorOptimization
{
  def apply(oper: Operator): Operator =
  {
    oper.recurExpressions(Eval.simplify(_))
        .recur(apply(_))
  }
}