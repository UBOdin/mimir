package mimir.optimizer;

import mimir.algebra._;

object FlattenBooleanConditionals extends TopDownExpressionOptimizerRule {

	def applyOne(e: Expression): Expression =
	{
		e match {

			case Conditional(condition, thenClause, elseClause) 
				if thenClause.equals(elseClause) =>
					thenClause

			case Conditional(condition, thenClause, elseClause) 
				if Typechecker.weakTypeOf(e) == TBool() =>
					ExpressionUtils.makeOr(
						ExpressionUtils.makeAnd(condition, thenClause),
						ExpressionUtils.makeAnd(
							ExpressionUtils.makeNot(condition), 
							elseClause
						)
					)

			case _ => e

		}
			
	}
}