package mimir.ctables

import java.sql.SQLException

import mimir.algebra._
import mimir.util._
import mimir.optimizer.PropagateConditions;


object CTPartition {

	def enumerateAllPartitions(clauses: Seq[Seq[Expression]]): Seq[Expression] = 
	{
		// TimeUtils.mark("Enumerate: "+clauses)
		clauses match {
			case List() => List(BoolPrimitive(true))
			case head :: rest => 
				head.flatMap ( (possibility) => 
					enumerateAllPartitions(rest).map( ExpressionUtils.makeAnd(possibility, _) )
				)
		}
	}

	def allCandidateConditions(e: Expression): Seq[Expression] =
	{
		// TimeUtils.mark("Conditions: "+e)
		if(!CTables.isProbabilistic(e)){
			return List(BoolPrimitive(true))
		}
		enumerateAllPartitions(
			(e match {
				case Conditional(condition, thenClause, elseClause) =>
					if(
						// If the condition column is probabilistic, then it
						// can't be used to differentiate between deterministic
						// and non-deterministic columns.
						CTables.isProbabilistic(condition)
						// Alternatively, if the then and else clauses are both
						// deterministic or both non-deterministic, then the 
						// condition can't be used to distinguish between the two
						  || (CTables.isProbabilistic(thenClause) ==
							  CTables.isProbabilistic(elseClause))
					) { 
						// So see what we can get from the children and move on
						None
					} else {
						// Otherwise, the condition is useful!
						Some[Seq[Expression]](List(condition, Not(condition)))
					}

				case _ => None
			}) match {
				case Some(conditions) => conditions :: e.children.map(allCandidateConditions(_)).toList
				case None => e.children.map(allCandidateConditions(_))
			}
		)
	}

	def createPartition(phi: Expression, partition: Expression): Option[(Expression, Expression)] =
	{
		val partitionCondition = 
			PropagateConditions.apply(
				ExpressionUtils.makeAnd(partition, phi)
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
							proj.columns.filter( !_.name.equals(CTables.conditionColumn) )
						val partitions = allCandidateConditions(phi);
						// println("Conditions: " + conditionCases)
						val partitionQueries: Seq[Operator] = 
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