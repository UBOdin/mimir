package mimir.algebra;

import mimir.ctables._;

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

object ExpressionOptimizer {

	val standardOptimizatins = List[ExpressionOptimizerRule](
		PullUpBranches,
		FlattenBooleanConditionals,
		PropagateConditions
	)

	def optimize(e:Expression, opts: List[ExpressionOptimizerRule]): Expression = 
		opts.foldLeft(e)( (currE, f) => f(currE) )

	def optimize(e: Expression): Expression =
		optimize(e, standardOptimizatins)

}