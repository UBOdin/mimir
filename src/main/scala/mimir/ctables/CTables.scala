package mimir.ctables

import mimir.algebra._
import mimir.models._
import scala.util._

object CTables 
{

  // Function names for calculating row probability, variance and confidence
  val ROW_PROBABILITY = "PROB"
  val VARIANCE = "VAR"
  val CONFIDENCE = "CONFIDENCE"

  val SEED_EXP = "__SEED"

  /**
   * Default name for a condition column
   */
  def conditionColumn = "__MIMIR_CONDITION"

  /**
   * Could the provided Expression be probabilistic?
   * 
   * Returns true if there is a VGTerm anywhere in it
   */
  def isProbabilistic(expr: Expression): Boolean = 
  expr match {
    case VGTerm(_, _, _, _) => true
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
    (oper match {
      case Project(cols, _) => 
        cols.exists( (x: ProjectArg) => isProbabilistic(x.expression) )
      case Select(expr, _) => 
        isProbabilistic(expr)
      case _ => false;
    }) || oper.children.exists( isProbabilistic(_) )
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

  def getVGTerms(e: Expression): Set[VGTerm] =
    getVGTerms(e: Expression, Map[String, PrimitiveValue]())

  def getVGTerms(e: Expression, bindings: Map[String, PrimitiveValue]): Set[VGTerm] =
  {
    val children: Set[VGTerm] = e.children.flatMap( getVGTerms(_) ).toSet
    e match {
      case v : VGTerm => children + v
      case _ => children
    }
  }
  def getVGTerms(oper: Operator): Set[VGTerm] = 
    (oper.expressions.flatMap(getVGTerms(_)) ++ 
          oper.children.flatMap(getVGTerms(_))).toSet
}