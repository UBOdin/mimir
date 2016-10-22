package mimir.optimizer;

import java.sql._;

import mimir.algebra._;
import mimir.ctables._;

object InlineVGTerms {

	def inline(e: Expression): Expression =
	{

		e match {
			case v @ VGTerm(model, idx, args) => 
				val simplifiedArgs = args.map(apply(_));

				if(simplifiedArgs.forall(_.isInstanceOf[PrimitiveValue])){
					v.get(simplifiedArgs.map(_.asInstanceOf[PrimitiveValue]))
				} else {
					v.rebuild(simplifiedArgs)
				}

			case _ => e.rebuild(e.children.map(inline(_)))
		}
	}

	def apply(e: Expression): Expression =
	{
		Eval.simplify(inline(e))
	}

	def apply(o: Operator): Operator = 
	{
		o match {

			case Project(cols, src) => 
				Project(
					cols.map( (col:ProjectArg) => ProjectArg(col.name, apply(col.expression)) ),
					apply(src)
				)

			case Select(cond, src) =>
				Select(apply(cond), apply(src))

			case _ => o.rebuild(o.children.map(apply(_)))

		}
	}

}