package mimir.optimizer.expression

import mimir.algebra._
import mimir.optimizer._

class FlattenBooleanConditionals(typechecker:Typechecker) extends TopDownExpressionOptimizerRule {

	def applyOne(e: Expression): Expression =
	{
		e match {

			case Conditional(condition, thenClause, elseClause) 
				if thenClause.equals(elseClause) =>
					thenClause

			case Conditional(condition, thenClause, elseClause) 
				if typechecker.weakTypeOf(e) == TBool() =>
					ExpressionUtils.makeOr(
						ExpressionUtils.makeAnd(condition, thenClause),
						ExpressionUtils.makeAnd(
							ExpressionUtils.makeNot(condition), 
							elseClause
						)
					)

      case Conditional(BoolPrimitive(true), thenCase, _) => thenCase

      case Conditional(BoolPrimitive(false), _, elseCase) => elseCase

			case _ => e

		}
			
	}
}
