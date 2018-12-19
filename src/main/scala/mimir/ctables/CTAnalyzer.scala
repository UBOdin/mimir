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
            Conditional(condition, thenDeterministic, elseDeterministic)
          )
        }
      }

      case Arithmetic(Arith.And, l, r) => {
        val lDeterministic = recur(l)
        val rDeterministic = recur(r)
        ExpressionUtils.makeOr(Seq(
          ExpressionUtils.makeAnd(
            lDeterministic,
            ExpressionUtils.makeNot(l)
          ),
          ExpressionUtils.makeAnd(
            rDeterministic,
            ExpressionUtils.makeNot(r)
          ),
          ExpressionUtils.makeAnd(
            lDeterministic,
            rDeterministic
          )
        ))
      }
      
      case Arithmetic(Arith.Or, l, r) => {
        val lDeterministic = recur(l)
        val rDeterministic = recur(r)
        ExpressionUtils.makeOr(Seq(
          ExpressionUtils.makeAnd(
            lDeterministic,
            l
          ),
          ExpressionUtils.makeAnd(
            rDeterministic,
            r
          ),
          ExpressionUtils.makeAnd(
            lDeterministic,
            rDeterministic
          )
        ))
      }

      case v: VGTerm =>
        if(v.args.isEmpty){
          BoolPrimitive(
            models(v.name).isAcknowledged(v.idx, Seq())
          )
        } else {
          IsAcknowledged(models(v.name), v.idx, v.args)
        }

      case w: DataWarning => 
        if(w.key.isEmpty){
          BoolPrimitive(
            models(w.name).isAcknowledged(0, Seq())
          )
        } else {
          IsAcknowledged(models(w.name), 0, w.key)
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
  def compileCausality(expr: Expression): Seq[(Expression, UncertaintyCausingExpression)] =
    compileCausality(expr, BoolPrimitive(true))

  private def compileCausality(expr: Expression, inputCondition: Expression): Seq[(Expression, UncertaintyCausingExpression)] = 
  {
    expr match { 
      case Conditional(condition, thenClause, elseClause) => {

        val conditionCausality = compileCausality(condition, inputCondition)

        val thenCondition = 
          ExpressionUtils.makeAnd(inputCondition, condition)

        val elseCondition =
          ExpressionUtils.makeAnd(inputCondition, ExpressionUtils.makeNot(condition))
          
        conditionCausality ++ 
          compileCausality(thenClause, thenCondition) ++
          compileCausality(elseClause, elseCondition)
      }
      //TODO: We should come up with a more complete way to compile causality
      //        for And and Or
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
      
      case x: UncertaintyCausingExpression => List( (inputCondition, x) )

      case _ => expr.children.flatMap(compileCausality(_, inputCondition))

    }
  }

  def compileSample(expr: Expression, seed: Expression, models: (String => Model)): Expression =
  {
    val replacement =
      expr match {
        case VGTerm(name, idx, args, hints) => 
          Sampler(models(name), idx, args, hints, seed)
        case _ => expr
      }

    return replacement.recur(compileSample(_, seed, models))

  }
}