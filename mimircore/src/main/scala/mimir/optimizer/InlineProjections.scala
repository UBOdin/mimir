package mimir.optimizer;

import java.sql._;

import mimir.algebra._;
import mimir.ctables._;

object InlineProjections {

	def optimize(o: Operator): Operator = 
	{
		o match {

			case Project(cols, src) if (cols.forall( _ match {
				case ProjectArg(colName, Var(varName)) => colName.equals(varName)
				case _ => false
			}) && (src.schema.map(_._1).toSet &~ cols.map(_.column).toSet).isEmpty)

			 => optimize(src)

			case Project(cols, p @ Project(_, src)) =>
				val bindings = p.bindings;
				optimize(Project(
					cols.map( (arg:ProjectArg) =>
						ProjectArg(arg.column, Eval.inline(arg.input, bindings))
					),
					src
				))

			case _ => o.rebuild(o.children.map(optimize(_)))

		}
	}

}