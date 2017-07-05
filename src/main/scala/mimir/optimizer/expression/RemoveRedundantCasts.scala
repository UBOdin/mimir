package mimir.optimizer.expression

import mimir.ctables._
import mimir.algebra._
import mimir.optimizer._

object RemoveRedundantCasts extends ExpressionOptimizerRule {

  def apply(e: Expression) =
  {
    e match {
      case Function("CAST", List(Function("CAST", List(target, _)), castType)) =>
        apply(Function("CAST", List(target, castType)))
      case _ => e.recur(apply(_))
    }
  }
}