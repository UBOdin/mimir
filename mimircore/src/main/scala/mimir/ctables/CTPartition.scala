package mimir.ctables

import java.sql.SQLException

import mimir.algebra._
import mimir.util._


object CTPartition {

	def extractCondition(precondition: Expression, wList: List[WhenThenClause], 
											 elseIsProbabilistic: Boolean): 
			(List[Expression], Boolean, Boolean) = 
	{
		wList match {
			case WhenThenClause(cond, effect) :: rest => 
				if(CTables.isProbabilistic(cond)) {
					(List(precondition), false, true)
				} else {
					val currentConditionCase =
						Arith.makeAnd(precondition, cond)
					val altCondition = 
						Arith.makeAnd(precondition, Arith.makeNot(cond))
					val (remainingConditionCases, hasDet, hasProb) =
						extractCondition(altCondition, rest, elseIsProbabilistic)
					( 
						currentConditionCase :: remainingConditionCases, 
						hasDet || !CTables.isProbabilistic(effect),
						hasProb || CTables.isProbabilistic(effect)
					)
				}
			case List() =>
				(List(precondition), !elseIsProbabilistic, elseIsProbabilistic)
		}
	}


	def extractConditions(e: Expression): List[List[Expression]] =
	{
		if(!CTables.isProbabilistic(e)){
			return List( List(BoolPrimitive(true)) )
		}
		val currentExpressionConditions = 
			e match {
				case CaseExpression(whenClauses, elseClause) =>
					val (cases, hasDet, hasProb) = 
						extractCondition(BoolPrimitive(true), whenClauses, CTables.isProbabilistic(elseClause))
					if(hasDet && hasProb){ Some(cases) } else { None }

				case _ => None
			}
		e.children.flatMap(extractConditions) ++ currentExpressionConditions
	}

	def enumeratePossibilities(clauses: List[List[Expression]]): List[Expression] = 
	{
		clauses match {
			case List() => List(BoolPrimitive(true))
			case head :: rest => 
				head.flatMap ( (possibility) => 
					enumeratePossibilities(rest).map( Arith.makeAnd(possibility, _) )
				)
		}
	}


}