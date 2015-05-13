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

  /** 
   * Returns two expressions for computing the lower and upper bounds 
   * (respectively) for a given expression.  
   * 
   * - For numeric values, the upper and lower bounds are defined intuitively.  
   * - For boolean values, False < True.  That is:
   *    - The upper bound evaluates to False iff the expression is 
   *      deterministically false.
   *    - The lower bound evaluates to True iff the expression is 
   *      deterministically true.
   */
  def compileForBounds(
      expr: Expression
    ): (Expression,Expression) =
  {
    if(!CTables.isProbabilistic(expr)){ return (expr,expr); }
    expr match {
//        case LeafExpression(_) => return (expr,expr)

      case Not(child) => compileForBounds(child) match { case (a,b) => (b,a) }

      case VGTerm((_,model),idx,args) => 
        model.boundsExpressions(idx, args)

      case Arithmetic(op, lhs, rhs) =>
        val (lhs_low, lhs_high) = compileForBounds(lhs);
        val (rhs_low, rhs_high) = compileForBounds(rhs);

        op match {
          case ( Arith.Add | Arith.And | Arith.Or ) =>
            return (Eval.inline(Arithmetic(op, lhs_low , rhs_low )), 
                    Eval.inline(Arithmetic(op, lhs_high, rhs_high)));

          case Arith.Sub => 
            return (Eval.inline(Arithmetic(Arith.Sub, lhs_low , rhs_high)),
                    Eval.inline(Arithmetic(Arith.Sub, lhs_high, rhs_low )));

          case ( Arith.Mult | Arith.Div ) =>

            val (num_options,expr_options) = List(
                (lhs_low,  rhs_low), (lhs_low,  rhs_high),
                (lhs_high, rhs_low), (lhs_high, rhs_high)
              ).distinct.map(
                _ match { case (x,y) => Eval.inline(Arithmetic(op, x, y)) } 
              ).partition( _ match { case IntPrimitive(_) | FloatPrimitive(_) => true 
                                     case _ => false }
              )

            val (num_options_low, num_options_high) = 
              if(num_options.isEmpty) { (List(), List()) }
              else { (
                  List(num_options.minBy( _.asInstanceOf[PrimitiveValue].asDouble ) ), 
                  List(num_options.maxBy( _.asInstanceOf[PrimitiveValue].asDouble ) )
              )}

            val options_low  = num_options_low  ++ expr_options
            val options_high = num_options_high ++ expr_options

            return (
              if(options_low.length > 1) { Function("__LIST_MIN", options_low) }
              else { options_low(0) }
              ,
              if(options_high.length > 1) { Function("__LIST_MAX", options_high) }
              else { options_high(0) }
            )
        }


    }
  }
}