package mimir.ctables

import mimir.algebra._
import mimir.lenses.MissingValueAnalysis

abstract class Model {
  def varTypes: List[Type.T]

  def mostLikelyValue   (idx: Int, args: List[PrimitiveValue]):  PrimitiveValue
  def lowerBound        (idx: Int, args: List[PrimitiveValue]):  PrimitiveValue
  def upperBound        (idx: Int, args: List[PrimitiveValue]):  PrimitiveValue
  def variance          (idx: Int, args: List[PrimitiveValue]):  PrimitiveValue
  def confidenceInterval(idx: Int, args: List[PrimitiveValue]):  PrimitiveValue
  def mostLikelyExpr    (idx: Int, args: List[Expression    ]):  Expression
  def lowerBoundExpr    (idx: Int, args: List[Expression    ]):  Expression
  def upperBoundExpr    (idx: Int, args: List[Expression    ]):  Expression
  def varianceExpr      (idx: Int, args: List[Expression    ]):  Expression
  def confidenceExpr    (idx: Int, args: List[Expression    ]):  Expression
  def sample(seed: Long, idx: Int, args: List[PrimitiveValue]):  PrimitiveValue
}

case class VGTerm(
  model: (String,Model), 
  idx: Int,
  args: List[Expression]
) extends Proc(args) {
  override def toString() = "{{ "+model._1+"_"+idx+"["+args.mkString(", ")+"] }}"
  override def exprType(bindings: Map[String, Type.T]):Type.T = model._2.varTypes(idx)
  override def children: List[Expression] = args
  override def rebuild(x: List[Expression]) = VGTerm(model, idx, x)
  def get(v: List[PrimitiveValue]): PrimitiveValue = model._2.mostLikelyValue(idx, v)
}

object CTables 
{

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
    case Function(_, _) => true
    case MissingValueAnalysis(_, _, _) => true
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

}