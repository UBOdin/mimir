package mimir.optimizer;

import java.sql._;

import mimir.algebra._;
import mimir.ctables._;

object InlineVGTerms {

	def inline(e: Expression): Expression =
	{

		e match {
			case v @ VGTerm(model, idx, args, hints) => 
				val simplifiedChildren = v.children.map(apply(_))

				if(simplifiedChildren.forall(_.isInstanceOf[PrimitiveValue])){
					v.get(simplifiedChildren.map(_.asInstanceOf[PrimitiveValue]))
				} else {
					v.rebuild(simplifiedChildren)
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
		o.recurExpressions(apply(_)).recur(apply(_))
	}

}