package mimir.ctables

import mimir.algebra._
import mimir.models._
import scala.util._

case class VGTerm(
  model: (String,Model), 
  idx: Int,
  args: List[Expression]
) extends Proc(args) {
  override def toString() = "{{ "+model._1+";"+idx+"["+args.mkString(", ")+"] }}"
  override def getType(bindings: List[Type.T]):Type.T = model._2.varType(idx, bindings)
  override def children: List[Expression] = args
  override def rebuild(x: List[Expression]) = VGTerm(model, idx, x)
  def get(v: List[PrimitiveValue]): PrimitiveValue = 
  {
    // println("VGTerm: Get")
    model._2.bestGuess(idx, v)
  }
  def reason(): Reason = 
    Reason(
      model._2.reason(idx, args),
      model._1,
      idx,
      args
    )
}

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
    case VGTerm(_, _, _) => true
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

  def getVGTerms(e: Expression): List[VGTerm] =
    getVGTerms(e: Expression, Map[String, PrimitiveValue]())

  def getVGTerms(e: Expression, bindings: Map[String, PrimitiveValue]): List[VGTerm] =
  {
    e match {
      case v : VGTerm => v :: e.children.flatMap( getVGTerms(_) )
      case _ => e.children.flatMap( getVGTerms(_) )
    }
  }
  def getVGTerms(oper: Operator): List[VGTerm] = 
    oper.expressions.flatMap(getVGTerms(_)) ++ 
      oper.children.flatMap(getVGTerms(_))
}