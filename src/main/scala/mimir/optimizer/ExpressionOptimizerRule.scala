package mimir.optimizer;

import mimir.ctables._;
import mimir.algebra._;

abstract class ExpressionOptimizerRule {
	def apply(e: Expression): Expression
}

abstract class TopDownExpressionOptimizerRule extends ExpressionOptimizerRule {
	def applyOne(e: Expression): Expression
	def apply(e: Expression): Expression = 
		applyOne(e).recur(apply _)
}

abstract class BottomUpExpressionOptimizerRule extends ExpressionOptimizerRule {
	def applyOne(e: Expression): Expression
	def apply(e: Expression): Expression = 
		applyOne(e.recur(apply _))
}
