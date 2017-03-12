package mimir.optimizer;

import mimir.algebra._;

object PushDownNots extends TopDownExpressionOptimizerRule {

  def applyOne(e: Expression): Expression =
  {
    e match {
      case Not(x) => ExpressionUtils.makeNot(x)
      case _ => e
    }
      
  }
}