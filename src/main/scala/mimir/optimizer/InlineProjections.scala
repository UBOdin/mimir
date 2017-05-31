package mimir.optimizer

import java.sql._
import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir.algebra._
import mimir.ctables._

object InlineProjections extends OperatorOptimization with LazyLogging {

	def apply(o: Operator): Operator = 
		o.recur(apply(_)) match {
			// If we have a Project[*](X), we can replace it with just X
			case Project(cols, src) if (cols.forall( _ match {
					case ProjectArg(colName, Var(varName)) => colName.equals(varName)
					case _ => false
				}) && (src.schema.map(_._1).toSet &~ cols.map(_.name).toSet).isEmpty)
				 => src

			// Project[...](Project[...](X)) can be composed into a single Project[...](X)
			case Project(cols, p @ Project(_, src)) =>
				val bindings = p.bindings;
				Project(
					cols.map( (arg:ProjectArg) =>
						ProjectArg(arg.name, Eval.inline(arg.expression, bindings))
					),
					src
				)

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
						Aggregate(groupBy, rewrittenAggs, src)
					}
					case None => Project(cols, agg)
				}
			}

			// Under some limited conditions, Aggregate[...](Project[...](X)) can be
			// composed into just Aggregate[...](X)
			case oldAgg @ Aggregate(groupBy, aggregates, p @ Project(cols, src)) => {
				val bindings = p.bindings
				val gbBindings = groupBy.flatMap { col =>
					bindings(col.name) match {
						case Var(newName) => 
							if(newName.equals(col.name)) { None } 
							else { Some( col.name -> Var(newName) ) }
						case _ => 
							// If there's a non variable rewrite, the projection needs to stay in place
							// Abort the rewrite right here
							return Aggregate(groupBy, aggregates, p)
					}
				}.toMap

				val rewrittenAggs = 
					aggregates.map( agg => 
						AggFunction(
							agg.function, 
							agg.distinct, 
							agg.args.map( arg => Eval.inline(arg, bindings) ),
							agg.alias
						)
					)

				// If we don't need to rewrite any group-by columns, the entire projection
				// can be folded into the aggregate.  Otherwise, we need a post-processing
				// step to rename the conflicting attributes.
				if(gbBindings.isEmpty){
					Aggregate(groupBy, rewrittenAggs, src)
				} else {
					Project(
						oldAgg.columnNames.map { col =>
							ProjectArg( col, gbBindings.getOrElse(col, Var(col)) )
						},
						Aggregate(
							groupBy.map { col => gbBindings.getOrElse(col.name, col) },
							rewrittenAggs,
							src
						)
					)
				}
			}

      case Join(lhs, rhs) if (lhs.isInstanceOf[Project] || rhs.isInstanceOf[Project]) => {
      	val (lhsCols, lhsBase) = OperatorUtils.extractProjections(lhs)
      	val (rhsCols, rhsBase) = OperatorUtils.extractProjections(rhs)
      	val (safeJoin, rhsRewrites) = 
          OperatorUtils.makeSafeJoin(lhsBase, rhsBase)
        val rhsRewriteMapping =
        	rhsRewrites.map { case (orig, rewritten) => (orig -> Var(rewritten)) }

        Typechecker.typecheck(safeJoin)

        Project(
          lhsCols ++
          	rhsCols.map { case ProjectArg(name, expression) =>
          		ProjectArg(name, Eval.inline(expression, rhsRewriteMapping))
          	},
          safeJoin
        )
      }

			// Otherwise, we might still be able to simplify the nested expressions
			// Do a quick Eval.inline pass over them.
			case Project(cols, src) => 
				Project(
					cols.map( (arg:ProjectArg) =>
						ProjectArg(arg.name, Eval.inline(arg.expression))
					),
					src
				)

			// If it's anything else, this optimization doesn't care 
			// (recursion is handled bottom-up at the start of this function)
			case anythingElse => anythingElse
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
				// If the renaming is non-unique...
				if(renaming.map(_._1).toSet.size != renaming.size){
					return None
				}


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
}