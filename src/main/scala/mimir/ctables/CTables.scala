package mimir.ctables

import mimir.algebra._
import mimir.util.JSONBuilder
import scala.util._

abstract class Model {
  /**
   * Infer the type of the model from the types of the inputs
   * @param argTypes    The types of the arguments the the VGTerm
   * @return            The type of the value returned by this model
   */
  def varType        (idx: Int, argTypes:List[Type]): Type

  /**
   * Generate a best guess for a variable represented by this model.
   * @param idx         The index of the variable family to generate a best guess for
   * @param args        The skolem identifier for the specific variable to generate a best guess for
   * @return            A primitive value representing the best guess value.
   */
  def bestGuess      (idx: Int, args: List[PrimitiveValue]):  PrimitiveValue
  /**
   * Generate a sample from the distribution of a variable represented by this model.
   * @param idx         The index of the variable family to generate a sample for
   * @param randomness  A java.util.Random to use when generating the sample (pre-seeded)
   * @param args        The skolem identifier for the specific variable to generate a sample for
   * @return            A primitive value representing the generated sample
   */
  def sample         (idx: Int, randomness: Random, args: List[PrimitiveValue]):  PrimitiveValue
  /**
   * Generate a human-readable explanation for the uncertainty captured by this model.
   * @param idx   The index of the variable family to explain
   * @param args  The skolem identifier for the specific variable to explain
   * @return      A string reason explaining the uncertainty in this model
   */
  def reason         (idx: Int, args: List[Expression]): (String)
}

case class Reason(
  val reason: String,
  val model: String,
  val idx: Int,
  val args: List[Expression]
){
  override def toString: String = 
    reason+" ("+model+"_"+idx+"["+args.mkString(", ")+"])"

  def toJSON: String =
    JSONBuilder.dict(Map(
      "english" -> JSONBuilder.string(reason),
      "source"  -> JSONBuilder.string(model),
      "varid"   -> JSONBuilder.int(idx),
      "args"    -> JSONBuilder.list( args.map( x => JSONBuilder.string(x.toString) ) )
    ))
}

case class VGTerm(
  model: (String,Model), 
  idx: Int,
  args: List[Expression]
) extends Proc(args) {
  override def toString() = "{{ "+model._1+"_"+idx+"["+args.mkString(", ")+"] }}"
  override def getType(bindings: List[Type]):Type = model._2.varType(idx, bindings)
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