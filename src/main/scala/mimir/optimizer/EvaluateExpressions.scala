package mimir.optimizer

import mimir.algebra._

object EvaluateExpressions
{
  def apply(oper: Operator): Operator =
  {
    oper.recurExpressions(Eval.simplify(_))
        .recur(apply(_))
  }
}