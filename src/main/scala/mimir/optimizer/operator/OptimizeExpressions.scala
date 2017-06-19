package mimir.optimizer.operator

import mimir.Database
import mimir.algebra._
import mimir.optimizer.OperatorOptimization

class OptimizeExpressions(optimize: Expression => Expression)
  extends OperatorOptimization
{
  def apply(oper: Operator): Operator =
  {
    oper.recurExpressions(optimize(_))
        .recur(apply(_))
  }
}