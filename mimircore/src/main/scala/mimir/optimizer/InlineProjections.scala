package mimir.optimizer;

import java.sql._;

import mimir.algebra._;
import mimir.ctables._;

object InlineProjections {

	def optimize(o: Operator): Operator = 
		o match {
			// If we have a Project[*](X), we can replace it with just X
			case Project(cols, src) if (cols.forall( _ match {
					case ProjectArg(colName, Var(varName)) => colName.equals(varName)
					case _ => false
				}) && (src.schema.map(_._1).toSet &~ cols.map(_.name).toSet).isEmpty)
				 => optimize(src)

			// Project[...](Project[...](X)) can be composed into a single Project[...](X)
			case Project(cols, p @ Project(_, src)) =>
				val bindings = p.bindings;
				optimize(Project(
					cols.map( (arg:ProjectArg) =>
						ProjectArg(arg.name, Eval.inline(arg.expression, bindings))
					),
					src
				))

			// Otherwise, we might still be able to simplify the nested expressions
			// Do a quick Eval.inline pass over them.
			case Project(cols, src) => 
				Project(
					cols.map( (arg:ProjectArg) =>
						ProjectArg(arg.name, Eval.inline(arg.expression))
					),
					optimize(src)
				)

			// If it's not a projection, this optimization doesn't care
			case _ => o.recur(optimize(_:Operator))
	}

}