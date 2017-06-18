package mimir.optimizer.operator

import mimir.Database
import mimir.algebra._
import mimir.optimizer.OperatorOptimization

class EvaluateExpressions(db: Database)
  extends OperatorOptimization
{
  def apply(oper: Operator): Operator =
  {
    oper.recurExpressions(db.compiler.optimize(_))
        .recur(apply(_))
  }
}