package mimir.optimizer;

import java.sql._;

import mimir.algebra._;
import mimir.ctables._;

object InlineProjections {

	def apply(o: Operator): Operator = 
		o match {
			// If we have a Project[*](X), we can replace it with just X
			case Project(cols, src) if (cols.forall( _ match {
					case ProjectArg(colName, Var(varName)) => colName.equals(varName)
					case _ => false
				}) && (src.schema.map(_._1).toSet &~ cols.map(_.name).toSet).isEmpty)
				 => apply(src)

			// Project[...](Project[...](X)) can be composed into a single Project[...](X)
			case Project(cols, p @ Project(_, src)) =>
				val bindings = p.bindings;
				apply(Project(
					cols.map( (arg:ProjectArg) =>
						ProjectArg(arg.name, Eval.inline(arg.expression, bindings))
					),
					src
				))

			// Under some limited conditions, Project[...](Aggregate[...](X)) can be
			// composed into just Aggregate[...](X)
			case Project(cols, agg @ Aggregate(groupBy, aggregates, src)) => {
				canInlineProjectAggregate(cols, groupBy, aggregates) match {
					case Some(renaming) => {
						val rewrittenAggs = 
							aggregates.map( agg => 
								AggFunction(
									agg.function, 
									agg.distinct, 
									agg.args, 
									renaming(agg.alias)
								)
							)
						Aggregate(groupBy, rewrittenAggs, apply(src))
					}
					case None => Project(cols, apply(agg))
				}
			}

			// Under some limited conditions, Aggregate[...](Project[...](X)) can be
			// composed into just Aggregate[...](X)
			case Aggregate(groupBy, aggregates, p @ Project(cols, src)) => {
				if(canInlineAggregateProject(groupBy, p)){
					val bindings = p.bindings
					val rewrittenAggs = 
						aggregates.map( agg => 
							AggFunction(
								agg.function, 
								agg.distinct, 
								agg.args.map( arg => Eval.inline(arg, bindings) ),
								agg.alias
							)
						)
					Aggregate(groupBy, rewrittenAggs, apply(src))
				} else {
					Aggregate(groupBy, aggregates, apply(p))
				}

			}

			// Otherwise, we might still be able to simplify the nested expressions
			// Do a quick Eval.inline pass over them.
			case Project(cols, src) => 
				Project(
					cols.map( (arg:ProjectArg) =>
						ProjectArg(arg.name, Eval.inline(arg.expression))
					),
					apply(src)
				)

			// If it's not a projection, this optimization doesn't care
			case _ => o.recur(apply(_:Operator))
	}

	def canInlineProjectAggregate(
			cols: Seq[ProjectArg], 
			gb: Seq[Var], 
			aggs: Seq[AggFunction]
	): Option[Map[String, String]] =
	{
		// We're allowed to inline Project(Aggregate(...)) if all of the following hold
		//  1) The project is a renaming (i.e., all Project expressions are vars)
    //  2) The projection schema is exactly the (renamed) aggregate schema
    //  3) The group by variables are identical

		// Check 1: Ensure that the projection is a strict renaming.
		extractRenaming(cols) match {
			case None => return None
			case Some(renaming) => {
				val (gbExpected, aggExpected) = renaming.splitAt(gb.size)

				// Check 3: We can't rename GB columns in the aggregate
				for( (gbProjectOut, gbProjectIn) <- gbExpected ){
					if(!gbProjectIn.equals(gbProjectOut) ){ return None }
				}

				// Check 2, Part 1: We can't re-order GB columns, and they'd best all be there
				for( (found, expected) <- gb.map(_.name).zip(gbExpected.map(_._1)) ){
					if(!found.equals(expected)){ return None; }
				}

				// Check 2, Part 2: Aggregate aliases can be changed.
				for( (found, expected) <- aggs.map(_.alias).zip(aggExpected.map(_._1)) ){
					if(!found.equals(expected)){ return None; }
				}

				// We're here.  Return a name mapping for the aggregate aliases
				return Some(aggExpected.toMap)
			}
		}

	}

	def extractRenaming(cols: Seq[ProjectArg]): Option[Seq[(String, String)]] =
	{
		Some(cols.map({
			case ProjectArg(out, Var(in)) => (in, out)
			case _ => return None
		}))
	}

	def canInlineAggregateProject(gb: Seq[Var], p: Project): Boolean =
	{
		gb.map(_.name).
			forall( gbVar => p.get(gbVar) match {
				case Some(Var(cmpVar))	=> gbVar.equals(cmpVar)
				case _ => false
			})
	}
}