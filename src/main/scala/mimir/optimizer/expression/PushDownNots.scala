package mimir.optimizer.expression

import mimir.algebra._
import mimir.optimizer._

object PushDownNots extends TopDownExpressionOptimizerRule {

  def applyOne(e: Expression): Expression =
  {
    e match {
      case Not(x) => ExpressionUtils.makeNot(x)
      case Conditional(Not(i), t, e) => Conditional(i, e, t)
      case _ => e
    }
      
  }
}