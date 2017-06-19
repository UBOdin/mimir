package mimir.ctables

import mimir.algebra._
import scala.util._
import mimir.models._
import mimir.ctables.vgterm._
import mimir.Database

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
  def compileDeterministic(expr: Expression, models: (String => Model)): Expression =
    compileDeterministic(expr, models, Map[String,Expression]())


  def compileDeterministic(expr: Expression, models: (String => Model), 
                           varMap: Map[String,Expression]): Expression =
  {
    val recur = (x:Expression) => compileDeterministic(x, models, varMap)
    expr match { 
      
      case Conditional(condition, thenClause, elseClause) => {
        val thenDeterministic = recur(thenClause)
        val elseDeterministic = recur(elseClause)
        if(thenDeterministic.equals(elseDeterministic)){
          thenDeterministic
        } else {
          ExpressionUtils.makeAnd(
            recur(condition), 
            Conditional(condition, recur(thenClause), recur(elseClause))
          )
        }
      }

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

      case v: VGTerm =>
        if(v.args.isEmpty){
          BoolPrimitive(
            models(v.name).isAcknowledged(v.idx, Seq())
          )
        } else {
          IsAcknowledged(models(v.name), v.idx, v.args)
        }
      
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
  def compileCausality(expr: Expression): Seq[(Expression, VGTerm)] =
    compileCausality(expr, BoolPrimitive(true))

  private def compileCausality(expr: Expression, inputCondition: Expression): Seq[(Expression, VGTerm)] = 
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
          compileCausality(elseClause, ExpressionUtils.makeNot(thenElseCondition))
      }

      case Arithmetic(Arith.And, l, r) => {
        (CTables.isDeterministic(l), CTables.isDeterministic(r)) match {
          case (true, true)   => List()
          case (false, true)  => 
            // if the RHS is deterministic, then the LHS is relevant if and 
            // only if r is true, since ? && F == F
            compileCausality(l, 
              ExpressionUtils.makeAnd(inputCondition, r)
            )
          case (true, false)  => 
            // if the LHS is deterministic, then the RHS is relevant if and 
            // only if r is true, since F && ? == F
            compileCausality(r, 
              ExpressionUtils.makeAnd(inputCondition, l)
            )
          case (false, false) => 
            compileCausality(l, inputCondition) ++ compileCausality(r, inputCondition)
        }
      }

      case Arithmetic(Arith.Or, l, r) => {
        (CTables.isDeterministic(l), CTables.isDeterministic(r)) match {
          case (true, true)   => List()
          case (false, true)  => 
            // if the RHS is deterministic, then the LHS is relevant if and 
            // only if r is false, since ? || T == T
            compileCausality(l, 
              ExpressionUtils.makeAnd(inputCondition, ExpressionUtils.makeNot(r))
            )
          case (true, false)  => 
            // if the LHS is deterministic, then the RHS is relevant if and 
            // only if r is true, since T || ? == T
            compileCausality(r, 
              ExpressionUtils.makeAnd(inputCondition, ExpressionUtils.makeNot(l))
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
      case VGTerm(model, idx, args, hints) => 
        Function("VGTERM_SAMPLE", Seq(
          StringPrimitive(model),
          IntPrimitive(idx),
          seed
        ) ++ args ++ hints)
      case _ => expr.rebuild(expr.children.map(compileSample(_, seed)))
    }
  }
}