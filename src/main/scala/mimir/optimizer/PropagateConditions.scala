package mimir.optimizer;

import mimir.ctables._;
import mimir.algebra._;

object PropagateConditions extends ExpressionOptimizerRule {

	def applyAssertion(assertion: Expression, target: Expression): Expression =
		applyAssertion(true, assertion, target)

	def applyAssertion(truth: Boolean, assertion: Expression, target: Expression): Expression =
	{
		assertion match { 
			case Not(e) => return applyAssertion(!truth, e, target)
			case Comparison(Cmp.Neq,a,b) => return applyAssertion(!truth, Comparison(Cmp.Eq,a,b), target)
			case _ => ()
		}
		if(truth) {
			// Some fast-path cases
			assertion match {
				case Comparison(Cmp.Eq, Var(c), e) =>
					return Eval.inline(target, Map((c, e)))
				case Comparison(Cmp.Eq, e, Var(c)) =>
					return Eval.inline(target, Map((c, e)))
				case IsNullExpression(Var(c)) =>
					return Eval.inline(target, Map((c, NullPrimitive())))
				case _ => ()
			}
		}
		hardInline(truth, assertion, target);
	}

	def hardInline(truth: Boolean, assertion: Expression, target: Expression): Expression =
	{
		target match {
			case a if a.equals(assertion)      => return BoolPrimitive(truth)
			case Not(a) if a.equals(assertion) => return BoolPrimitive(!truth)
			case Comparison(op, lhs, rhs) if (Comparison(Cmp.negate(op), lhs, rhs)).equals(assertion)
										       => return BoolPrimitive(!truth)
			case _ => 
				return target.rebuild(
					target.children.map( 
						hardInline(truth, assertion, _) 
					)
				)
		}
	}

	def isUsefulAssertion(e: Expression): Boolean =
	{
		val isSimpler = (e: Expression) => (
			(!CTables.isProbabilistic(e)) && 
			ExpressionUtils.getColumns(e).isEmpty
		  )
		e match {
			case Comparison(Cmp.Eq, Var(c), e) => isSimpler(e)
			case Comparison(Cmp.Eq, e, Var(c)) => isSimpler(e)
			case Comparison(Cmp.Neq, a, b) => 
				isUsefulAssertion(Comparison(Cmp.Eq, a, b))
			case IsNullExpression(_) => true
			case Not(IsNullExpression(_)) => true
			case Not(Not(e1)) => isUsefulAssertion(e1)
			case _ => false
		}
	}

	def apply(e: Expression): Expression = 
		ExpressionUtils.makeAnd(propagateConditions(ExpressionUtils.getConjuncts(e)))

	def propagateConditions(l: List[Expression]): List[Expression] = 
	{
		l match {
			case head :: rest =>
				val applyInliner: (Boolean, Expression, Expression) => Expression = 
					if(isUsefulAssertion(head)){ applyAssertion _ } 
					else                       { hardInline _ }
				val newRest = 
					rest.map( applyInliner(true, head, _) ).
						 map( Eval.inline(_) )
				head :: propagateConditions(newRest)
			case List() => List()
		}
	}
}