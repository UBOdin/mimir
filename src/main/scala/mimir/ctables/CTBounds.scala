package mimir.ctables;

import mimir.algebra._

object CTBounds {
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
  def compile(
      expr: Expression
    ): (Expression,Expression) =
  {
    if(!CTables.isProbabilistic(expr)){ return (expr,expr); }
    expr match {
//        case LeafExpression(_) => return (expr,expr)

      case Not(child) => compile(child) match { case (a,b) => (b,a) }

      case VGTerm((_,model),idx,args) => 
        (model.lowerBoundExpr(idx, args), model.upperBoundExpr(idx, args))

      case Arithmetic(op, lhs, rhs) =>
        val (lhs_low, lhs_high) = compile(lhs);
        val (rhs_low, rhs_high) = compile(rhs);

        op match {
          case ( Arith.Add | Arith.And | Arith.Or ) =>
            return (Eval.inline(Arithmetic(op, lhs_low , rhs_low )), 
                    Eval.inline(Arithmetic(op, lhs_high, rhs_high)));

          case Arith.Sub => 
            return (Eval.inline(Arithmetic(Arith.Sub, lhs_low , rhs_high)),
                    Eval.inline(Arithmetic(Arith.Sub, lhs_high, rhs_low )));

          case ( Arith.Mult | Arith.Div ) =>

            val options = List(
                (lhs_low,  rhs_low), (lhs_low,  rhs_high),
                (lhs_high, rhs_low), (lhs_high, rhs_high)
              ).map(
                _ match { case (x,y) => 
                	val curr = Eval.inline(Arithmetic(op, x, y)) 
                	(curr, curr)
                } 
              )
            return combinePossibilities(options);
        }
      case CaseExpression(wtClauses, eClause) =>

      	return compileWhenThenClauses(wtClauses, eClause)

      case Comparison(op, lhs, rhs) =>

      	val (lhs_low, lhs_high) = compile(lhs)
      	val (rhs_low, rhs_high) = compile(rhs)

      	val (true_if_always, 
      		 false_if_impossible
      		) = op match {
	      		case Cmp.Eq => (
	      			Arithmetic(Arith.And,
		      			Arithmetic(Arith.And,
			      			Comparison(Cmp.Eq, lhs_low, lhs_high),
			      			Comparison(Cmp.Eq, rhs_low, rhs_high)
			      		),
		      			Comparison(Cmp.Eq, lhs_high, rhs_high)
		      		),
		      		Arithmetic(Arith.And,
		      			Comparison(Cmp.Gte, rhs_high, lhs_low),
		      			Comparison(Cmp.Gte, lhs_high, rhs_low)
		      		)
	      		)
	      		case Cmp.Neq => (
		      		Arithmetic(Arith.Or,
		      			Comparison(Cmp.Lte, lhs_low, rhs_high),
		      			Comparison(Cmp.Lte, rhs_low, lhs_high)
		      		),
		      		Arithmetic(Arith.Or,
		      			Arithmetic(Arith.Or,
			      			Comparison(Cmp.Neq, lhs_low, lhs_high),
			      			Comparison(Cmp.Neq, rhs_low, rhs_high)
			      		),
		      			Comparison(Cmp.Neq, lhs_high, rhs_high)
		      		)
	      		)
	      		case Cmp.Lt => (
	      			Comparison(Cmp.Lt,  lhs_high, rhs_low),
	      			Comparison(Cmp.Lte, lhs_low,  rhs_high)
	      		)
	      		// case Cmp.Lte => (
	      		// 	Comparison(Cmp.Lte, lhs_low,  rhs_high),
	      		// 	Comparison(Cmp.Gte, lhs_high, rhs_low)
	      		// )
	      		// case Cmp.Gt => (
	      		// 	Comparison(Cmp.Lt,  rhs_low,  lhs_high),
	      		// 	Comparison(Cmp.Gte, rhs_high, lhs_low)
	      		// )
	      		// case Cmp.Gte => (
	      		// 	Comparison(Cmp.Lte, rhs_low,  lhs_high),
	      		// 	Comparison(Cmp.Gte, rhs_high, lhs_low)
	      		// )
	      	}
      	println("FALSE: "+true_if_always + " -> " + Eval.inline(true_if_always))
      	println("TRUE: "+false_if_impossible + " -> " + Eval.inline(false_if_impossible))
 	    return (Eval.inline(true_if_always), Eval.inline(false_if_impossible))

    }
  }

  def combinePossibilities(options: List[(Expression,Expression)]): 
  	(Expression,Expression)=
  {
  	val (input_options_low, input_options_high) = options.unzip

  	val (num_options_low, expr_options_low) = 
  		input_options_low.distinct.
      		partition( _ match { case IntPrimitive(_) 
      								| FloatPrimitive(_) => true 
	                             case _ => false } )
	val num_low = 
		if(num_options_low.isEmpty) { List() }
		else { 
          List(num_options_low.minBy( _.asInstanceOf[PrimitiveValue].asDouble ) )
		}

  	val (num_options_high, expr_options_high) = 
  		input_options_high.distinct.
      		partition( _ match { case IntPrimitive(_) 
      								| FloatPrimitive(_) => true 
	                             case _ => false } )
	val num_high =
		if(num_options_high.isEmpty) { List() }
		else { 
          List(num_options_high.maxBy( _.asInstanceOf[PrimitiveValue].asDouble ) )
        }

    val options_low = num_low ++ expr_options_low;
    val options_high = num_high ++ expr_options_high;
    return (
      if(options_low.length > 1) { Function("__LIST_MIN", options_low) }
      else { options_low(0) }
      ,
      if(options_high.length > 1) { Function("__LIST_MAX", options_high) }
      else { options_high(0) }
    )

  }


  def compileWhenThenClauses(wtClauses: List[WhenThenClause], eClause: Expression): 
  	(Expression, Expression) =
  {
  	wtClauses match {

  		// Implement by stripping a WhenThenClause off the head of the
  		// list and processing it...
  		case WhenThenClause(w, t) :: rest => {
  			val w_low, w_high = compile(w);
  			if(w_high.equals(BoolPrimitive(false))){

  				// If the when clause is deterministically false, it 
  				// can be safely ignored.
  				return compileWhenThenClauses(rest, eClause)
  			} else {

  				if(w_low.equals(BoolPrimitive(true))){
  					
  					// If the when clause is deterministically true, then
  					// we don't need to recur -- the then-clause is 
  					// sufficient
  					return compile(t)

  				} else {

  					// Otherwise, combine the result of recurring down 
  					// the rest of the when/then clauses and the current
  					// bounds
  					combinePossibilities(List(
  						compile(t), 
  						compileWhenThenClauses(rest, eClause)
  					))
  				}

  			}
  		}

  		// If there are no when clauses, then this case statement
  		// just resolves to the else clause.
  		case _ => return compile(eClause)

  	}
  }

}