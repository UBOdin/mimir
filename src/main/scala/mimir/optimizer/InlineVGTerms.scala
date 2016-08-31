package mimir.optimizer;

import java.sql._;

import mimir.algebra._;
import mimir.ctables._;

object InlineVGTerms {

	def inline(e: Expression): Expression =
	{

		e match {
			case v @ VGTerm(model, idx, args) => 
				val simplifiedArgs = args.map(optimize(_));

				if(simplifiedArgs.forall(_.isInstanceOf[PrimitiveValue])){
					v.get(simplifiedArgs.map(_.asInstanceOf[PrimitiveValue]))
				} else {
					v.rebuild(simplifiedArgs)
				}

			case _ => e.rebuild(e.children.map(inline(_)))
		}
	}

	def optimize(e: Expression): Expression =
	{
		Eval.simplify(inline(e))
	}

	def optimize(o: Operator): Operator = 
	{
		o match {

			case Project(cols, src) => 
				Project(
					cols.map( (col:ProjectArg) => ProjectArg(col.name, optimize(col.expression)) ),
					optimize(src)
				)

			case Select(cond, src) =>
				Select(optimize(cond), optimize(src))

			case _ => o.rebuild(o.children.map(optimize(_)))

		}
	}

}