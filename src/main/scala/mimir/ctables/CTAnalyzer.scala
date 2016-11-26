package mimir.ctables

import mimir.algebra._
import scala.util._
import mimir.models._
import java.sql.SQLException

case class VGTermSampler(model: Model, idx: Int, args: List[Expression], seed: Expression) 
  extends Proc(  (seed :: args)  )
{
  def getType(argTypes: List[Type.T]): Type.T =
    model.varType(idx, argTypes)
  def get(v: List[PrimitiveValue]): PrimitiveValue = {
    v match {
      case seed :: argValues => model.sample(idx, new Random(seed.asLong), argValues)
      case _ => throw new SQLException("Internal error.  Expecting seed.")
    }
  }
  def rebuild(v: List[Expression]) = 
  {
    v match { 
      case seed :: argValues => VGTermSampler(model, idx, argValues, seed)
      case _ => throw new SQLException("Internal error.  Expecting seed.")
    }
  }

}


object CTAnalyzer {

  /**
   * Construct a boolean expression that evaluates whether the input 
   * expression is deterministic <b>for a given input row</b>.  
   * Note the corner-case here: CASE (aka branch) statements can
   * create some tuples that are deterministic and others that are
   * non-deterministic (depending on which branch is taken).  
   *
   * Base cases: <ul>
   *   <li>VGTerm is false</li>
   *   <li>Var | Const is true</li>
   * </ul>
   * 
   * Everything else (other than CASE) is an AND of whether the 
   * child subexpressions are deterministic
   */
  def compileDeterministic(expr: Expression): Expression =
    compileDeterministic(expr, Map[String,Expression]())


  def compileDeterministic(expr: Expression, 
                           varMap: Map[String,Expression]): Expression =
  {
    val recur = (x:Expression) => compileDeterministic(x, varMap)
    expr match { 
      
      case Conditional(condition, thenClause, elseClause) =>
        ExpressionUtils.makeAnd(
          recur(condition), 
          Conditional(condition, recur(thenClause), recur(elseClause))
        )

      case Arithmetic(Arith.And, l, r) =>
        ExpressionUtils.makeOr(
          ExpressionUtils.makeAnd(
            recur(l),
            ExpressionUtils.makeNot(l)
          ),
          ExpressionUtils.makeAnd(
            recur(r),
            ExpressionUtils.makeNot(r)
          )
        )
      
      case Arithmetic(Arith.Or, l, r) =>
        ExpressionUtils.makeOr(
          ExpressionUtils.makeAnd(
            recur(l),
            l
          ),
          ExpressionUtils.makeAnd(
            recur(r),
            r
          )
        )

      case _: VGTerm =>
        BoolPrimitive(false)
      
      case Var(v) => 
        varMap.get(v).getOrElse(BoolPrimitive(true))
      
      case _ => expr.children.
                  map( recur(_) ).
                  fold(
                    BoolPrimitive(true)
                  )( 
                    ExpressionUtils.makeAnd(_,_) 
                  )
    }
  }

  /**
   * Find all VGTerms that appear in the expression, and compute a set of conditions
   * under which each of those terms affect the result.  Similar to compileDeterministic,
   * but on a term-by-term basis.
   */
  def compileCausality(expr: Expression): List[(Expression, VGTerm)] =
    compileCausality(expr, BoolPrimitive(true))

  private def compileCausality(expr: Expression, inputCondition: Expression): List[(Expression, VGTerm)] = 
  {
    expr match { 
      case Conditional(condition, thenClause, elseClause) => {

        val conditionCausality = compileCausality(condition, inputCondition)

        val thenElseCondition = 
          if(CTables.isDeterministic(condition)){ 
            ExpressionUtils.makeAnd(inputCondition, condition)
          } else {
            inputCondition
          }

        conditionCausality ++ 
          compileCausality(thenClause, thenElseCondition) ++
          compileCausality(elseClause, thenElseCondition)
      }

      case Arithmetic(Arith.And, l, r) => {
        (CTables.isDeterministic(l), CTables.isDeterministic(r)) match {
          case (true, true)   => List()
          case (false, true)  => 
            compileCausality(l, 
              ExpressionUtils.makeAnd(inputCondition, ExpressionUtils.makeNot(r))
            )
          case (true, false)  => 
            compileCausality(r, 
              ExpressionUtils.makeAnd(inputCondition, ExpressionUtils.makeNot(l))
            )
          case (false, false) => 
            compileCausality(l, inputCondition) ++ compileCausality(r, inputCondition)
        }
      }

      case Arithmetic(Arith.Or, l, r) => {
        (CTables.isDeterministic(l), CTables.isDeterministic(r)) match {
          case (true, true)   => List()
          case (false, true)  => 
            compileCausality(l, 
              ExpressionUtils.makeAnd(inputCondition, r)
            )
          case (true, false)  => 
            compileCausality(r, 
              ExpressionUtils.makeAnd(inputCondition, l)
            )
          case (false, false) => 
            compileCausality(l, inputCondition) ++ compileCausality(r, inputCondition)
        }
      }

      case x: VGTerm => List( (inputCondition, x) )

      case _ => expr.children.flatMap(compileCausality(_, inputCondition))

    }
  }

  def compileSample(expr: Expression, seed: Expression): Expression =
  {
    expr match {
      case VGTerm((_,model), idx, args) => VGTermSampler(model, idx, args, seed)
      case _ => expr.rebuild(expr.children.map(compileSample(_, seed)))
    }
  }
}