package mimir.algebra;

import mimir.ctables._;

object PullUpBranches extends TopDownExpressionOptimizerRule {

	def applyOne(e: Expression) =
	{
		e match {
			case Comparison(op, Conditional(condition, thenClause, elseClause), rhs) =>
				Conditional(condition, Comparison(op, thenClause, rhs), 
									   Comparison(op, elseClause, rhs))
			case Comparison(op, lhs, Conditional(condition, thenClause, elseClause)) =>
				Conditional(condition, Comparison(op, lhs, thenClause), 
									   Comparison(op, lhs, elseClause))
			case Arithmetic(op, Conditional(condition, thenClause, elseClause), rhs) =>
				Conditional(condition, Arithmetic(op, thenClause, rhs), 
									   Arithmetic(op, elseClause, rhs))
			case Arithmetic(op, lhs, Conditional(condition, thenClause, elseClause)) =>
				Conditional(condition, Arithmetic(op, lhs, thenClause), 
									   Arithmetic(op, lhs, elseClause))
			case _ => e
		}
	}
}