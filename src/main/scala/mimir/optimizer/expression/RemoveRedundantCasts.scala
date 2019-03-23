package mimir.optimizer.expression

import mimir.ctables._
import mimir.algebra._
import mimir.optimizer._

object RemoveRedundantCasts extends ExpressionOptimizerRule {

  def apply(e: Expression) =
  {
    e match {
      case Function(ID("cast"), List(Function(ID("cast"), List(target, _)), castType)) =>
        apply(Function(ID("cast"), List(target, castType)))
      case _ => e.recur(apply(_))
    }
  }
}