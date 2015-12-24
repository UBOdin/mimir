package mimir.ctables

import java.sql.SQLException

import mimir.algebra._
import mimir.util._


object CTPartition {

	def extractCondition(precondition: Expression, wList: List[WhenThenClause], 
											 elseIsProbabilistic: Boolean): 
			(List[Expression], Boolean, Boolean) = 
	{
		// println("Precondition: " + precondition)
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


	def enumerateAllPartitions(clauses: List[List[Expression]]): List[Expression] = 
	{
		// TimeUtils.mark("Enumerate: "+clauses)
		clauses match {
			case List() => List(BoolPrimitive(true))
			case head :: rest => 
				head.flatMap ( (possibility) => 
					enumerateAllPartitions(rest).map( Arith.makeAnd(possibility, _) )
				)
		}
	}

	def allCandidateConditions(e: Expression): List[Expression] =
	{
		// TimeUtils.mark("Conditions: "+e)
		if(!CTables.isProbabilistic(e)){
			return List(BoolPrimitive(true))
		}
		enumerateAllPartitions(
			(e match {
				case CaseExpression(whenClauses, elseClause) =>
					val (cases, hasDet, hasProb) = 
						extractCondition(BoolPrimitive(true), whenClauses, CTables.isProbabilistic(elseClause))
					if(hasDet && hasProb){ Some(cases) } else { None }

				case _ => None
			}) match {
				case Some(conditions) => conditions :: e.children.map(allCandidateConditions(_))
				case None => e.children.map(allCandidateConditions(_))
			}
		)
	}

	def createPartition(phi: Expression, partition: Expression): Option[(Expression, Expression)] =
	{
		val partitionCondition = 
			ExpressionOptimizer.propagateConditions(
				Arith.makeAnd(partition, phi)
			)
		partitionCondition match {
			case BoolPrimitive(false) => None
			case _ =>	Some(CTables.extractProbabilisticClauses(partitionCondition))
		}
	}

	def partition(f: Operator): Operator =
	{
		if(!CTables.isProbabilistic(f)){ return f; }
		f match { 
			case proj : Project =>
				if(CTables.isProbabilistic(proj.source)){
					throw new SQLException("Partitioning a non-normalized project expression:"+f)
				}
				proj.get(CTables.conditionColumn) match {
					case Some(phi) => {
						val nonConditions = 
							proj.columns.filter( !_.getColumnName.equals(CTables.conditionColumn) )
						val partitions = allCandidateConditions(phi);
						// println("Conditions: " + conditionCases)
						val partitionQueries = 
							partitions.flatMap( createPartition(phi, _) ).
		          	map( { case (nondet: Expression, det: Expression) => {
		          		val outerCondition = 
		          			nondet match { 
		          				case BoolPrimitive(true) => None
		          				case _ => Some(ProjectArg(CTables.conditionColumn, nondet))
		          			}
		          		val innerQuery = 
		          			det match {
		          				case BoolPrimitive(true) => proj.source
		          				case _ => Select(det, proj.source)
		          			}
		          		Project(nonConditions ++ outerCondition, innerQuery)
		           	}})
		        return OperatorUtils.makeUnion(partitionQueries)
					}
					case None =>
						return f;
				}
			case Union(lhs, rhs) =>
				Union(partition(lhs), partition(rhs))

			case _ => 
				throw new SQLException("Partitioning a non-normalized expression")
		}
	}


}