package mimir.optimizer;

import java.sql._;

import mimir.algebra._;
import mimir.ctables._;

object PushdownSelections {

	def optimize(o: Operator): Operator = 
	{
		o match {
			case Project(cols, src) =>
				val (conditionCol, rest) =
					cols.partition(_.getColumnName == CTables.conditionColumn)
				if(conditionCol.isEmpty) {
					// If there's no condition column, just recur
					return Project(cols, optimize(src))
				} else {
					// If there is a condition column, convert it to a selection
					return Project(cols, optimize(Select(conditionCol.head.input, src)))
				}
			case Select(cond1, Select(cond2, src)) =>
				optimize(Select(Arith.makeAnd(cond1, cond2), src))

			case Select(cond, (p @ Project(cols, src))) =>
				optimize(Project(cols, Select(Eval.inline(cond, p.bindings), src)))

			case Select(cond, Union(isAll, lhs, rhs)) =>
				Union(isAll, optimize(Select(cond, lhs)), optimize(Select(cond, rhs)))

			case Select(_, (_:Table)) => o

			case Select(cond, Join(lhs, rhs)) => {
				val clauses: List[Expression] = Arith.getConjuncts(cond)
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

				val lhsCond = Arith.makeAnd(lhsClauses ++ genericClauses)
				val rhsCond = Arith.makeAnd(rhsClauses ++ genericClauses)
				val outerCond = Arith.makeAnd(rhsRest)

				val wrap = (selectCond: Expression, child: Operator) => {
					selectCond match {
						case BoolPrimitive(true) => child
						case _ => Select(selectCond, child)
					}
				}

				wrap(outerCond, Join(
					optimize(wrap(lhsCond, lhs)),
					optimize(wrap(rhsCond, rhs))
				))
			}
				

			case Select(_,_) =>
				throw new SQLException("Unhandled Select Case in Pushdown: " + o)

			case _ => o.rebuild(o.children.map(optimize(_)))

		}
	}

}