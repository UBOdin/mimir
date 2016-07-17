package mimir.ctables

import mimir.algebra._
import scala.util._
import java.sql.SQLException

case class VGTermSampler(model: Model, idx: Int, args: List[Expression], seed: Expression) 
  extends Proc(  (seed :: args)  )
{
  def getType(argTypes: List[Type.T]): Type.T =
    model.varTypes(idx)
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
        Arith.makeAnd(
          recur(condition), 
          Conditional(condition, recur(thenClause), recur(elseClause))
        )

      case Arithmetic(Arith.And, l, r) =>
        Arith.makeOr(
          Arith.makeAnd(
            recur(l),
            Not(l)
          ),
          Arith.makeAnd(
            recur(r),
            Not(r)
          )
        )
      
      case Arithmetic(Arith.Or, l, r) =>
        Arith.makeOr(
          Arith.makeAnd(
            recur(l),
            l
          ),
          Arith.makeAnd(
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
                    Arith.makeAnd(_,_) 
                  )
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