package mimir.optimizer;

import java.sql._;

import mimir.algebra._;
import mimir.ctables._;

object PushdownSelections {

	def wrap(selectCond: Expression, child: Operator): Operator = {
		selectCond match {
			case BoolPrimitive(true) => child
			case _ => Select(selectCond, child)
		}
	}

	def apply(o: Operator): Operator = 
	{
		o match {
			case Project(cols, src) =>
				// println("Cols: "+cols.map(_.getColumnName))
				val (conditionCol, rest) =
					cols.partition(_.name == CTables.conditionColumn)
				if(conditionCol.isEmpty) {
					// println("NOT CONVERTING")
					// If there's no condition column, just recur
					return Project(cols, apply(src))
				} else {
					// println("CONVERTING")
					// If there is a condition column, convert it to a selection
					return Project(rest, apply(Select(conditionCol.head.expression, src)))
				}
			case Select(cond1, Select(cond2, src)) =>
				apply(Select(ExpressionUtils.makeAnd(cond1, cond2), src))

			case Select(cond, (p @ Project(cols, src))) => {
				Eval.inline(cond, p.bindings) match {
					case BoolPrimitive(true) => apply(Project(cols, src))
					case BoolPrimitive(false) => EmptyTable(o.schema)
					case newCond => apply(Project(cols, Select(newCond, src)))
				}
			}

			case Select(cond, Union(lhs, rhs)) =>
				Union(apply(Select(cond, lhs)), apply(Select(cond, rhs)))

			case Select(_, (_:Table)) => o

			case Select(cond, Join(lhs, rhs)) => {
				val clauses: Seq[Expression] = ExpressionUtils.getConjuncts(cond)
				val lhsSchema = lhs.schema.map(_._1).toSet
				val rhsSchema = rhs.schema.map(_._1).toSet
				val dualSchema = lhsSchema ++ rhsSchema

				if(! (lhsSchema & rhsSchema).isEmpty ){
					throw new SQLException("Pushdown into overlapping schema ("+(lhsSchema&rhsSchema)+"): "+o)
				}
				if(! (ExpressionUtils.getColumns(cond) &~ dualSchema).isEmpty ){
					throw new SQLException("Pushdown into unsafe schema")
				}

				// Generic clauses have no variable references.  In principle, 
				// there should be no such clauses, but let's be safe.
				val (genericClauses: List[Expression], genericRest: List[Expression]) =
					clauses.partition(
						(x: Expression) => ExpressionUtils.getColumns(x).isEmpty
					)

				// Left-hand-side clauses are the ones where there's no overlap
				// with variables from the right-hand-side
				val (lhsClauses: List[Expression], lhsRest: List[Expression]) =
					genericRest.partition(
						(x: Expression) => (ExpressionUtils.getColumns(x) & rhsSchema).isEmpty
					)
				// Right-hand-side clauses are the ones where there's no overlap
				// with variables from the right-hand-side
				val (rhsClauses: List[Expression], rhsRest: List[Expression]) =
					lhsRest.partition(
						(x: Expression) => (ExpressionUtils.getColumns(x) & lhsSchema).isEmpty
					)

				val lhsCond = ExpressionUtils.makeAnd(lhsClauses ++ genericClauses)
				val rhsCond = ExpressionUtils.makeAnd(rhsClauses ++ genericClauses)
				val outerCond = ExpressionUtils.makeAnd(rhsRest)

				wrap(outerCond, Join(
					apply(wrap(lhsCond, lhs)),
					apply(wrap(rhsCond, rhs))
				))
			}

			case Select(cond, LeftOuterJoin(lhs, rhs, outerJoinCond)) => {
				val clauses: Seq[Expression] = ExpressionUtils.getConjuncts(cond)
				val rhsSchema = rhs.schema.map(_._1).toSet

				// Left-hand-side clauses are the ones where there's no overlap
				// with variables from the right-hand-side
				val (lhsClauses: Seq[Expression], lhsRest: Seq[Expression]) =
					clauses.partition(
						(x: Expression) => (ExpressionUtils.getColumns(x) & rhsSchema).isEmpty
					)

				val lhsCond = ExpressionUtils.makeAnd(lhsClauses)
				val outerCond = ExpressionUtils.makeAnd(lhsRest)

				wrap(outerCond, 
					LeftOuterJoin(
						wrap(lhsCond, lhs), 
						rhs, 
						outerJoinCond
					)
				)
			}

			// We're allowed to push selects down through aggregates if and only if 
			// all of the variables that appear in the condition appear in the group-by
			// clauses.
			case Select(cond, Aggregate(gbcols, aggs, source)) 
				if ExpressionUtils.getColumns(cond).map(Var(_)).forall( gbcols contains _ ) => {
				Aggregate(gbcols, aggs, apply(Select(cond, source)))
			}

			// Otherwise, leave the select in place
			case Select(cond, agg:Aggregate) => {
				Select(cond, apply(agg))
			}
			
			case Select(cond, EmptyTable(sch)) => EmptyTable(sch)

			case Select(cond, View(name, query, annotations)) => 
				Select(cond, View(name, apply(query), annotations))

			case Select(_,_) => 
				throw new SQLException("Unhandled Select Case in Pushdown: " + o)

			case _ => o.rebuild(o.children.map(apply(_)))

		}
	}

}