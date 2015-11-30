package mimir.algebra;

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
		if(target.equals(assertion)) {
			return BoolPrimitive(truth)
		} else {
			return target.rebuild(
						target.children.map( 
							applyAssertion(truth, assertion, _) 
						)
					)
		}
	}

	def propagateConditions(e: Expression): Expression = 
		Arith.makeAnd(propagateConditions(Arith.getConjuncts(e)))

	def propagateConditions(l: List[Expression]): List[Expression] = 
	{
		l match {
			case head :: rest =>
				val newRest = 
					rest.map( applyAssertion(head, _) ).
						 map( Eval.inline(_) )
				head :: propagateConditions(newRest)
			case List() => List()
		}
	}

}