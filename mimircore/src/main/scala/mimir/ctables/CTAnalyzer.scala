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

  def compileVariance(exp: Expression): Expression = {
      exp match {
        case Not(child) => compileVariance(child)

        case VGTerm((_,model),idx,args) => model.varianceExpr(idx, args)

        case CaseExpression(wtClauses, eClause) => {
          var wt = List[WhenThenClause]();
          for(i <- wtClauses.indices){
            val tclause = wtClauses(i).then match {
              case Var(_) => IntPrimitive(0)
              case _ => compileVariance(wtClauses(i).then)
            }
            wt ::= WhenThenClause(wtClauses(i).when, tclause)
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

  def compileConfidence(exp: Expression): (Expression, Expression) = {
    exp match {
      case Not(child) => compileConfidence(child) match { case (a,b) => (b,a) }

      case VGTerm((str,model),idx,args) =>
        (Arithmetic(Arith.Sub, model.mostLikelyExpr(idx, args), model.confidenceExpr(idx, args)),
          Arithmetic(Arith.Add, model.mostLikelyExpr(idx, args), model.confidenceExpr(idx, args)))

      case CaseExpression(wtClauses, eClause) => {
        var wtmin = List[WhenThenClause]()
        var wtmax = List[WhenThenClause]()
        for(i <- wtClauses.indices){
          val wclause = compileConfidence(wtClauses(i).when)
          val tclause = compileConfidence(wtClauses(i).then)
          wtmin ::= WhenThenClause(wclause._1, tclause._1)
          wtmax ::= WhenThenClause(wclause._2, tclause._2)
        }
        val e = compileConfidence(eClause)
        (CaseExpression(wtmin, e._1), CaseExpression(wtmax, e._2))
      }

      case Var(a) => (Var(a), Var(a))

      case nullExp @ IsNullExpression(child, neg) => (nullExp, nullExp)

      case p: PrimitiveValue => (p, p)
    }
  }

  def compileSample(exp: Expression, seedExp: Expression = null): Expression = {
    exp match {
        case Not(child) => Not(compileSample(child, seedExp))

        case VGTerm((_,model),idx,args) => {
          if(seedExp == null)
            model.sampleGenExpr(idx, args)
          else model.sampleGenExpr(idx, args ++ List(seedExp))
        }

        case CaseExpression(wtClauses, eClause) => {
          var wt = List[WhenThenClause]()
          for(i <- wtClauses.indices){
            val wclause = compileSample(wtClauses(i).when,seedExp)
            val tclause = compileSample(wtClauses(i).then, seedExp)
            wt ::= WhenThenClause(wclause, tclause)
          }
          CaseExpression(wt, compileSample(eClause, seedExp))
        }

        case Arithmetic(o, l, r) => Arithmetic(o, compileSample(l, seedExp), compileSample(r, seedExp))

        case Comparison(o, l, r) => Comparison(o, compileSample(l, seedExp), compileSample(r, seedExp))

        case Var(a) => Var(a)

        case IsNullExpression(child, neg) => IsNullExpression(compileSample(child, seedExp), neg)

        case p: PrimitiveValue => p
    }
  }
}