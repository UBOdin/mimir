package mimir.optimizer;

import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir.ctables._;
import mimir.algebra._;

object PropagateConditions extends OperatorOptimization with LazyLogging {

	def applyAssertion(assertion: Expression, target: Expression): Expression =
		applyAssertion(true, assertion, target)

	def applyAssertion(truth: Boolean, assertion: Expression, target: Expression): Expression =
	{
		logger.trace(s"applyAssertion(${if(truth){""}else{"!"}}$assertion -> $target")
		assertion match { 
			case Not(e) => return applyAssertion(!truth, e, target)
			case Comparison(Cmp.Neq,a,b) => return applyAssertion(!truth, Comparison(Cmp.Eq,a,b), target)
			case _ => ()
		}
		if(truth) {
			// Some fast-path cases
			assertion match {
				case Comparison(Cmp.Eq, Var(v1), Var(v2)) =>
					// For variable replacements, and for sanity's sake give preference to the shorter name
					if(v1.length <= v2.length){
						return Eval.inline(target, Map(v2 -> Var(v1)))
					} else {
						return Eval.inline(target, Map(v1 -> Var(v2)))
					}
				case Comparison(Cmp.Eq, Var(c), e) =>
					return Eval.inline(target, Map(c -> e))
				case Comparison(Cmp.Eq, e, Var(c)) =>
					return Eval.inline(target, Map(c -> e))
				case IsNullExpression(Var(c)) =>
					return Eval.inline(target, Map(c -> NullPrimitive()))
				case _ => ()
			}
		}
		hardInline(truth, assertion, target);
	}

	def hardInline(truth: Boolean, assertion: Expression, target: Expression): Expression =
	{
		target match {
			case a if a.equals(assertion)      => return BoolPrimitive(truth)
			case Not(a) if a.equals(assertion) => return BoolPrimitive(!truth)
			case Comparison(op, lhs, rhs) if (Comparison(Cmp.negate(op), lhs, rhs)).equals(assertion)
										       => return BoolPrimitive(!truth)
			case _ => 
				return target.rebuild(
					target.children.map( 
						hardInline(truth, assertion, _) 
					)
				)
		}
	}

	def isUsefulAssertion(e: Expression): Boolean =
	{
		val isSimpler = (e: Expression) => (
			(!CTables.isProbabilistic(e)) && 
			ExpressionUtils.getColumns(e).isEmpty
		  )
		e match {
			case Comparison(Cmp.Eq, Var(_), Var(_)) => true
			case Comparison(Cmp.Eq, Var(c), e) => isSimpler(e)
			case Comparison(Cmp.Eq, e, Var(c)) => isSimpler(e)
			case Comparison(Cmp.Neq, a, b) => 
				isUsefulAssertion(Comparison(Cmp.Eq, a, b))
			case IsNullExpression(_) => true
			case Not(IsNullExpression(_)) => true
			case Not(Not(e1)) => isUsefulAssertion(e1)
			case _ => false
		}
	}

	def apply(e: Expression, assertions: Seq[Expression] = Seq()): Expression = 
	{
		logger.debug(s"Apply([${assertions.mkString("; ")}] -> $e)")
		assertions.foldRight(
			propagateConditions(e)
		) { 
			applyAssertion(_, _)
		}.recur(apply(_, assertions))
	}

  def recur(o: Operator): (Operator, Seq[Expression]) =
  {
  	logger.debug(s"Propagating conditions in $o")
  	o match {
  		case Select(cond, src) =>
  			val (rewrittenSelect, srcAssertions) = recur(src)
  			logger.debug(s"Propagating: $srcAssertions into $cond")
  			val rewrittenCond = apply(cond, srcAssertions)
  			logger.debug(s"   -> $rewrittenCond")
  			(
  				Select(rewrittenCond, rewrittenSelect), 
  				ExpressionUtils.getConjuncts(cond).filter(isUsefulAssertion(_)) ++ srcAssertions
				)
			case Join(lhs, rhs) =>
				val (lhsRewriten, lhsAssertions) = recur(lhs)
				val (rhsRewritten, rhsAssertions) = recur(rhs)
				(
					Join(lhsRewriten, rhsRewritten),
					lhsAssertions ++ rhsAssertions
				)
			case _ => 
				val rewrite = 
					o.children match {
						case Seq(src) => 
			  			val (rewritten, srcAssertions) = recur(src)
			  			logger.debug(s"Rewriting \n$o\nwith $srcAssertions")
		  				o.recurExpressions(apply(_, srcAssertions))
		  				 .rebuild(Seq(rewritten))

			  		case _ =>
			  			o.recur(apply(_))
	  			} 
  			logger.debug(s"Replacing with\n$rewrite")
	  		(rewrite, Seq())
  	}
  }

	def apply(o: Operator): Operator =
		recur(o)._1


	def propagateConditions(e: Expression): Expression =
		ExpressionUtils.makeAnd(
			propagateConditions(
				ExpressionUtils.getConjuncts(e)
					.map{ _.recur(propagateConditions(_)) }
			)
		)


	def propagateConditions(l: Seq[Expression]): Seq[Expression] = 
		propagateConditions(l.toList)

	private def propagateConditions(l: List[Expression]): List[Expression] = 
	{

		l match {
			case head :: rest =>
				val applyInliner: (Boolean, Expression, Expression) => Expression = 
					if(isUsefulAssertion(head)){ applyAssertion _ } 
					else                       { hardInline _ }
				val newRest = 
					rest.map( applyInliner(true, head, _) ).
						 map( Eval.inline(_) )
				head :: propagateConditions(newRest)
			case List() => List()
		}
	}
}