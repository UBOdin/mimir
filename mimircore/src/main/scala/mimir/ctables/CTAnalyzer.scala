package mimir.ctables

import mimir.algebra._

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

  def compileSample(exp: Expression, seedExp: Expression = null): Expression = {
    exp match {

        case VGTerm((_,model),idx,args) => {
          if(seedExp == null)
            model.sampleGenExpr(idx, args)
          else model.sampleGenExpr(idx, args ++ List(seedExp))
        }

        case _ => exp.rebuild(exp.children.map(compileSample(_, seedExp)))
    }
  }
}