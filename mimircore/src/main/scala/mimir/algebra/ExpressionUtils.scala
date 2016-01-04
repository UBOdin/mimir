package mimir.algebra;

object ExpressionUtils {

	def getColumns(e: Expression): Set[String] = {
		e match {
			case Var(id) => Set(id)
			case _ => e.children.
						map(getColumns(_)).
						foldLeft(Set[String]())(_++_)
		}
	}

	def makeCaseExpression(whenThenClauses: List[(Expression, Expression)], 
						   elseClause: Expression): Expression =
	{
		whenThenClauses.foldRight(elseClause)( (wt, e) => 
			Conditional(wt._1, wt._2, e)
		)
	}

	def foldConditionalsToCase(e: Expression): 
		(List[(Expression, Expression)], Expression) =
	{
		foldConditionalsToCase(e, BoolPrimitive(true))
	}

	def foldConditionalsToCase(e: Expression, prefix: Expression): 
		(List[(Expression, Expression)], Expression) =
	{
		e match { 
			case Conditional(condition, thenClause, elseClause) =>
			{
				val thenCondition = Arith.makeAnd(prefix, condition)
				val (thenWhenThens, thenElse) = 
					foldConditionalsToCase(thenClause, thenCondition)
				val (elseWhenThens, elseElse) = 
					foldConditionalsToCase(elseClause, condition)
				(	
					thenWhenThens ++ List((thenCondition, thenElse)) ++ elseWhenThens, 
					elseElse
				)
			}
			case _ => (List[(Expression, Expression)](), e)
		}
	}

}