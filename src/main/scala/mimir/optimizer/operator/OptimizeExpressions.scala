package mimir.optimizer.operator

import mimir.Database
import mimir.algebra._
import mimir.algebra.function._
import mimir.optimizer.OperatorOptimization
import mimir.optimizer.expression._

class OptimizeExpressions(optimize: Expression => Expression)
  extends OperatorOptimization
{
  def apply(oper: Operator): Operator =
  {
    oper.recurExpressions(optimize(_))
        .recur(apply(_))
  }
}

object SimpleOptimizeExpressions 
  extends OptimizeExpressions(
    Seq(
      PullUpBranches,
      PushDownNots,
      RemoveRedundantCasts,
      new SimplifyExpressions(null, new FunctionRegistry())
    ).foldLeft(_) { case (expr, optimization) => optimization(expr) }
  )