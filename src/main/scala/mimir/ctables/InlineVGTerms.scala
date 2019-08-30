package mimir.ctables

import java.sql._

import mimir.Database
import mimir.algebra._
import mimir.ctables._

object InlineVGTerms {

	private def assembleMultiFragmentKey(key: Seq[Expression]) =
		key match {
			case Seq() => IntPrimitive(1) 
			case Seq(loneKey) => loneKey
			case multiKey => Function(ID("concat"), multiKey.map { CastExpression(_, TString()) })
		}

	def inline(e: Expression, db: Database): Expression =
	{

		e match {
			case IsAcknowledged(lens, key) => 
				if(db.lenses.areAllAcknowledged(lens)){ BoolPrimitive(true) }
				else {
					ExpressionUtils.makeInTest(
						assembleMultiFragmentKey(key),
						db.lenses.acknowledgedKeys(lens).map { 
							assembleMultiFragmentKey(_)
						}
					)
				}

			case Caveat(_, v, _, _) => v.recur(inline(_, db))

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