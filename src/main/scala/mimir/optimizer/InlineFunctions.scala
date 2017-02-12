package mimir.optimizer;

import mimir.ctables._;
import mimir.algebra._;

/**
 * Remove obvious no-ops and other expressions that can be evaluated
 * inline or replaced with something simpler
 */
object InlineFunctions extends BottomUpExpressionOptimizerRule {

  def applyOne(e: Expression) =
  {
    e match {
      case Function("MIMIR_MAKE_ROWID", Seq(x))            => x

      case Function("CAST", Seq(x, TypePrimitive(TAny()))) => x

      case Function(fname, args) => 
        FunctionRegistry.unfold(fname, args) match {
          case Some(unfolded) => unfolded
          case None => e
        }


      case _ => e
    }
  }
}