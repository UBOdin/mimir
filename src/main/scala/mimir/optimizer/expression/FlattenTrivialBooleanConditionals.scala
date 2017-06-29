package mimir.optimizer.expression

import mimir.algebra._
import mimir.optimizer._

class FlattenTrivialBooleanConditionals(typechecker: Typechecker) extends TopDownExpressionOptimizerRule {

  def applyOne(e: Expression): Expression =
  {
    e match {

      case Conditional(condition, thenClause, elseClause) 
        if thenClause.equals(elseClause) =>
          thenClause

      case Conditional(condition, thenCase:BoolPrimitive, elseCase:BoolPrimitive) 
        if typechecker.weakTypeOf(e) == TBool() =>
          ExpressionUtils.makeOr(
            ExpressionUtils.makeAnd(condition, thenCase),
            ExpressionUtils.makeAnd(
              ExpressionUtils.makeNot(condition), 
              elseCase
            )
          )

      case Conditional(BoolPrimitive(true), thenCase, _) => thenCase

      case Conditional(BoolPrimitive(false), _, elseCase) => elseCase

      case _ => e

    }
      
  }
}