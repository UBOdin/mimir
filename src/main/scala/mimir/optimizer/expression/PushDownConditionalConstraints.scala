package mimir.optimizer.expression

import mimir.algebra._
import mimir.optimizer._

/**
 * In applying lenses, Mimir creates a lot of nested constraints that are tricky
 * to evaluate if taken literally, but can be vastly simplified if you consider
 * the implied constraints on each branch of the conditional.  For example
 * 
 *   IF x IS NULL THEN IF x IS NULL ... 
 * can be simplified to
 *   IF x IS NULL ...
 * 
 * Specifically, this rule checks for conditional conditions of the form:
 *  - x IS NULL
 *  - x IS NOT NULL
 *  - x = [const]
 *  - x <> [const]
 * 
 * These constraints are propagated down as possible.
 */
object PushDownConditionalConstraints extends TopDownExpressionOptimizerRule {

  def guaranteeNotNull(v: ID, e: Expression): Expression =
  {
    e match {
      case IsNullExpression(Var(v2)) if v.equals(v2) => BoolPrimitive(false)
      case _ => e.recur(guaranteeNotNull(v, _))
    }
  }

  def applyOne(e: Expression): Expression =
  {
    e match {

      case Conditional(test@IsNullExpression(Var(v)), thenClause, elseClause) => 
        Conditional(
          test, 
          Eval.inline(thenClause, Map(v -> NullPrimitive())),
          guaranteeNotNull(v, elseClause)
        )
      
      case Conditional(test@Not(IsNullExpression(Var(v))), thenClause, elseClause) => 
        Conditional(
          test, 
          guaranteeNotNull(v, thenClause),
          Eval.inline(elseClause, Map(v -> NullPrimitive()))
        )

      case Conditional(test@Comparison(Cmp.Eq, Var(v), prim:PrimitiveValue), thenCase, elseCase) =>
        Conditional(
          test,
          Eval.inline(thenCase, Map(v -> prim)),
          elseCase
        )

      case Conditional(test@Comparison(Cmp.Eq, prim:PrimitiveValue, Var(v)), thenCase, elseCase) =>
        Conditional(
          test,
          Eval.inline(thenCase, Map(v -> prim)),
          elseCase
        )

      case Conditional(test@Comparison(Cmp.Neq, Var(v), prim:PrimitiveValue), thenCase, elseCase) =>
        Conditional(
          test,
          thenCase,
          Eval.inline(elseCase, Map(v -> prim))
        )

      case Conditional(test@Comparison(Cmp.Neq, prim:PrimitiveValue, Var(v)), thenCase, elseCase) =>
        Conditional(
          test,
          thenCase,
          Eval.inline(elseCase, Map(v -> prim))
        )

      case _ => e

    }
      
  }
}