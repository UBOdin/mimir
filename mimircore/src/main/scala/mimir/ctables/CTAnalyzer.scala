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
  {
    if(!CTables.isProbabilistic(expr)){
      return BoolPrimitive(true)
    }
    expr match { 
      
      case CaseExpression(caseClauses, elseClause) =>
        CaseExpression(
          caseClauses.map( (clause) =>
            WhenThenClause(
              Arith.makeAnd(
                compileDeterministic(clause.when),
                clause.when
              ),
              compileDeterministic(clause.then)
            )
          ),
          compileDeterministic(elseClause)
        )
      
      
      case Arithmetic(Arith.And, l, r) =>
        Arith.makeOr(
          Arith.makeAnd(
            compileDeterministic(l),
            Not(l)
          ),
          Arith.makeAnd(
            compileDeterministic(r),
            Not(r)
          )
        )
      
      case Arithmetic(Arith.Or, l, r) =>
        Arith.makeOr(
          Arith.makeAnd(
            compileDeterministic(l),
            l
          ),
          Arith.makeAnd(
            compileDeterministic(r),
            r
          )
        )
      
      case _: VGTerm =>
        BoolPrimitive(false)
      
      case Var(v) => 
        BoolPrimitive(true)
      

      case _ => expr.children.
                  map( compileDeterministic(_) ).
                  fold(
                    BoolPrimitive(true)
                  )( 
                    Arith.makeAnd(_,_) 
                  )
    }
  }

  //TODO check this
  def compileVariance(exp: Expression): Expression = {
    /*if(!CTables.isProbabilistic(exp))
      exp
    else*/
      exp match {
        case Not(child) => compileVariance(child)

        case VGTerm((_,model),idx,args) => model.varianceExpr(idx, args)

        case CaseExpression(wtClauses, eClause) => {
          var wt = List[WhenThenClause]();
          for(i <- wtClauses.indices){
            val wclause = compileVariance(wtClauses(i).when)
            val tclause = wtClauses(i).then match {
              case Var(_) => IntPrimitive(0)
              case _ => compileVariance(wtClauses(i).then)
            }
            wt ::= WhenThenClause(wclause, tclause)
          }
          val e = eClause match {
            case Var(_) => IntPrimitive(0)
            case _ => compileVariance(eClause)
          }
          CaseExpression(wt, e)
        }

        case Var(a) => Var(a)

        case IsNullExpression(child, neg) => IsNullExpression(compileVariance(child), neg)
      }
  }

  def compileConfidence(exp: Expression): Expression = {
    exp match {
      case Not(child) => compileConfidence(child)

      case VGTerm((_,model),idx,args) => model.varianceExpr(idx, args)

      case CaseExpression(wtClauses, eClause) => {
        var wt = List[WhenThenClause]();
        for(i <- wtClauses.indices){
          val wclause = compileConfidence(wtClauses(i).when)
          val tclause = compileConfidence(wtClauses(i).then)
          wt ::= WhenThenClause(wclause, tclause)
        }
        val e = compileConfidence(eClause)
        CaseExpression(wt, e)
      }

      case Var(a) => Var(a)

      case IsNullExpression(child, neg) => IsNullExpression(compileVariance(child), neg)
    }
  }
}