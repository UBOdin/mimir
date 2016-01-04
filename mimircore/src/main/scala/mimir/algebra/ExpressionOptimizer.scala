package mimir.algebra;

import mimir.ctables._;

object ExpressionOptimizer {

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
		if(target.equals(assertion)) {
			return BoolPrimitive(truth)
		} else {
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

	def propagateConditions(e: Expression): Expression = 
		Arith.makeAnd(propagateConditions(Arith.getConjuncts(e)))

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

	def optimize(e:Expression): Expression = 
		List[Expression => Expression](
			propagateConditions(_)
		).foldLeft(e)( (ex,f) => f(ex) )

}