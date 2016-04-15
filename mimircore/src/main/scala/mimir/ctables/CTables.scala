package mimir.ctables

import mimir.algebra._
import mimir.lenses.{TypeInferenceAnalysis, MissingValueAnalysis}

abstract class Model {
  def varTypes: List[Type.T]

  def mostLikelyValue   (idx: Int, args: List[PrimitiveValue]):  PrimitiveValue
  def lowerBound        (idx: Int, args: List[PrimitiveValue]):  PrimitiveValue
  def upperBound        (idx: Int, args: List[PrimitiveValue]):  PrimitiveValue
  def sampleGenerator   (idx: Int, args: List[PrimitiveValue]):  PrimitiveValue
  def mostLikelyExpr    (idx: Int, args: List[Expression    ]):  Expression
  def lowerBoundExpr    (idx: Int, args: List[Expression    ]):  Expression
  def upperBoundExpr    (idx: Int, args: List[Expression    ]):  Expression
  def sampleGenExpr     (idx: Int, args: List[Expression    ]):  Expression
  def sample            (seed: Long, idx: Int, args: List[PrimitiveValue]):  PrimitiveValue
  def reason            (idx: Int, args: List[Expression]): (String, String)
  def backingStore      (idx: Int): String
}

case class VGTerm(
  model: (String,Model), 
  idx: Int,
  args: List[Expression]
) extends Proc(args) {
  override def toString() = "{{ "+model._1+"_"+idx+"["+args.mkString(", ")+"] }}"
  override def getType(bindings: List[Type.T]):Type.T = model._2.varTypes(idx)
  override def children: List[Expression] = args
  override def rebuild(x: List[Expression]) = VGTerm(model, idx, x)
  def get(v: List[PrimitiveValue]): PrimitiveValue = 
  {
    // println("VGTerm: Get")
    model._2.mostLikelyValue(idx, v)
  }
  def reason(): (String, String) = model._2.reason(idx, args)
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
    case MissingValueAnalysis(_, _, _) => true
    case TypeInferenceAnalysis(_, _, _) => true
    case Function("JOIN_ROWIDS", _) => expr.children.exists( isProbabilistic(_) )
    case Function("CAST", _) => expr.children.exists( isProbabilistic(_) )
    case Function("DATE", _) => false
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
        cols.exists( (x: ProjectArg) => isProbabilistic(x.input) )
      case Select(expr, _) => 
        isProbabilistic(expr)
      case _ => false;
    }) || oper.children.exists( isProbabilistic(_) )
  }
  
  def extractProbabilisticClauses(e: Expression): 
    (Expression, Expression) =
  {
    e match {
      case Arithmetic(Arith.And, lhs, rhs) =>
        val (lhsExtracted, lhsRemaining) = 
          extractProbabilisticClauses(lhs)
        val (rhsExtracted, rhsRemaining) = 
          extractProbabilisticClauses(rhs)
        ( Arith.makeAnd(lhsExtracted, rhsExtracted),
          Arith.makeAnd(lhsRemaining, rhsRemaining)
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

}