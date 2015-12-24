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

	def mergeCaseClauses(e: Expression): Expression =
	{
		e match {
			// CASE WHEN X THEN A WHEN Y THEN A ... => 
			// CASE WHEN X OR Y THEN A ...
			case CaseExpression(
				WhenThenClause(w1,t1) :: 
				WhenThenClause(w2,t2) :: rest, 
				elseClause) if t1 == t2 =>
					mergeCaseClauses(
						CaseExpression(
							WhenThenClause(Arith.makeOr(w1,w2),t1) :: rest, 
							elseClause
						)
					)

			// CASE WHEN X THEN A ELSE A END => A
			case CaseExpression(
				List(WhenThenClause(w, t)), e) if t == e => t

			// CASE ... 
			case CaseExpression(head :: rest, elseClause) =>
				mergeCaseClauses(CaseExpression(rest, elseClause)) match {
					case CaseExpression(newRest, newElseClause) =>
						CaseExpression(head :: newRest, newElseClause)
					case newElseClause => CaseExpression(List(head), newElseClause)
				}

			case CaseExpression(List(), elseClause) => elseClause

			case _ => e.recur(mergeCaseClauses)
		}
	}


	def optimize(e:Expression): Expression = 
		List[Expression => Expression](
			propagateConditions(_), 
			mergeCaseClauses(_)
		).foldLeft(e)( (ex,f) => f(ex) )

}