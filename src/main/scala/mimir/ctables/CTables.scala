package mimir.ctables

import mimir.algebra._
import mimir.util.JSONBuilder
import scala.util._

object CTables 
{

  val FN_BEST_GUESS   = ID("VGTERM_BEST_GUESS")
  val FN_SAMPLE       = ID("VGTERM_SAMPLE")
  val FN_IS_ACKED     = ID("VGTERM_IS_ACKNOWLEDGED")
  val FN_TEMP_ENCODED = ID("MIMIR_ENCODED_VGTERM")
  val SEED_EXP        = ID("__SEED")

  /**
   * Default name for a condition column
   */
  def conditionColumn = ID("__MIMIR_CONDITION")

  /**
   * Could the provided Expression be probabilistic?
   * 
   * Returns true if there is a VGTerm anywhere in it
   */
  def isProbabilistic(expr: Expression): Boolean = 
  expr match {
    case _:UncertaintyCausingExpression => true
    case _ => expr.children.exists( isProbabilistic(_) )
  }

  /**
   * Could the provided Operator be probabilistic?
   *
   * Returns true if there is a VGTerm referenced
   * by the provided Operator or its children
   */
  def isProbabilistic(oper: Operator): Boolean = 
  {
    oper.expressions.exists( isProbabilistic(_) ) ||
      oper.children.exists( isProbabilistic(_) )
  }


  def isDeterministic(expr:Expression): Boolean = !isProbabilistic(expr)
  def isDeterministic(oper:Operator): Boolean = !isProbabilistic(oper)
  
  def extractProbabilisticClauses(e: Expression): 
    (Expression, Expression) =
  {
    e match {
      case Arithmetic(Arith.And, lhs, rhs) =>
        val (lhsExtracted, lhsRemaining) = 
          extractProbabilisticClauses(lhs)
        val (rhsExtracted, rhsRemaining) = 
          extractProbabilisticClauses(rhs)
        ( ExpressionUtils.makeAnd(lhsExtracted, rhsExtracted),
          ExpressionUtils.makeAnd(lhsRemaining, rhsRemaining)
        )
      case _ => 
        if(isProbabilistic(e)){
          (e, BoolPrimitive(true))
        } else {
          (BoolPrimitive(true), e)
        }
    }
  }

  def getUncertainty(e: Expression): Set[UncertaintyCausingExpression] =
    getUncertainty(e: Expression, Map[String, PrimitiveValue]())

  def getUncertainty(
    e: Expression, 
    bindings: Map[String, PrimitiveValue]
  ): Set[UncertaintyCausingExpression] =
  {
    val children: Set[UncertaintyCausingExpression] = 
      e.children.flatMap( getUncertainty(_) ).toSet
    e match {
      case v : UncertaintyCausingExpression => children + v
      case _ => children
    }
  }
  def getUncertainty(oper: Operator): Set[UncertaintyCausingExpression] = 
    (oper.expressions.flatMap(getUncertainty(_)) ++ 
          oper.children.flatMap(getUncertainty(_))).toSet
}