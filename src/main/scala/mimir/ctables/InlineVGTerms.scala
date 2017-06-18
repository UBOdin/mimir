package mimir.ctables

import java.sql._

import mimir.Database
import mimir.algebra._
import mimir.ctables._
import mimir.ctables.vgterm._

object InlineVGTerms {

	def inline(e: Expression, db: Database): Expression =
	{

		e match {
			case v @ VGTerm(model, idx, args, hints) => {
				val simplifiedChildren = v.children.map(apply(_, db))
				val ret = v.rebuild(simplifiedChildren)

				if(
					ret.args.forall(_.isInstanceOf[PrimitiveValue])
					&& ret.hints.forall(_.isInstanceOf[PrimitiveValue])
				) { 
					val model = db.models.get(v.name)
					val args = ret.args.map { _.asInstanceOf[PrimitiveValue] }
					val hints = ret.args.map { _.asInstanceOf[PrimitiveValue] }

					model.bestGuess(v.idx, args, hints)
				} else { 
					BestGuess(
						db.models.get(ret.name),
						ret.idx,
						ret.args,
						ret.hints
					)
				}
			}

			case _ => e.recur(inline(_, db))
		}
	}

	def apply(e: Expression, db: Database): Expression =
	{
		inline(e, db)
	}

	def apply(o: Operator, db: Database): Operator = 
	{
		o.recurExpressions(apply(_, db)).recur(apply(_, db))
	}

}