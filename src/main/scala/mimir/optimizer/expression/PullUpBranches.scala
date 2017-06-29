package mimir.optimizer.expression

import mimir.ctables._
import mimir.algebra._
import mimir.optimizer._

/**
 * Optimization that moves conditionals higher in the expression tree.
 * 
 * For example: X > (IF Y = 2 THEN 5 ELSE 3) 
 * becomes: IF Y = 2 THEN X > 5 ELSE X > 3
 * which another rewrite turns into: ((Y = 2) AND (X > 5)) OR ((Y = 2) AND (X > 3))
 *
 * This is analogous to inlining in imperative languages.  The main perk is that
 * it exposes new opportunities for expression evaluation, particularly related to
 * simplification of expressions.  The drawback is that it can create more complex 
 * expressions, since the non-conditional side needs to be duplicated.  
 * 
 * This is particularly bad for conditionals, since (1) they have three separate
 * subtrees that get cloned, and (2) they're already hard to optimize around.
 * As a resut, we only apply this optimization to expressions where the LHS or RHS is
 * a non-conditional
 */
object PullUpBranches extends TopDownExpressionOptimizerRule {

	def containsConditional(e: Expression): Boolean =
	{
		e match { 
			case Conditional(_,_,_) => true
			case _ => e.children.exists(containsConditional(_))
		}
	}

	def applyOne(e: Expression) =
	{
		e match {
			case Comparison(op, Conditional(condition, thenClause, elseClause), rhs)
				if(!containsConditional(rhs)) => 
				Conditional(condition, Comparison(op, thenClause, rhs), 
									   Comparison(op, elseClause, rhs))
			case Comparison(op, lhs, Conditional(condition, thenClause, elseClause))
				if(!containsConditional(lhs)) => 
				Conditional(condition, Comparison(op, lhs, thenClause), 
									   Comparison(op, lhs, elseClause))
			case Arithmetic(op, Conditional(condition, thenClause, elseClause), rhs)
				if(!containsConditional(rhs) && !Arith.isBool(op)) => 
				Conditional(condition, Arithmetic(op, thenClause, rhs), 
									   Arithmetic(op, elseClause, rhs))
			case Arithmetic(op, lhs, Conditional(condition, thenClause, elseClause))
				if(!containsConditional(lhs) && !Arith.isBool(op)) => 
				Conditional(condition, Arithmetic(op, lhs, thenClause), 
									   Arithmetic(op, lhs, elseClause))
			case _ => e
		}
	}
}