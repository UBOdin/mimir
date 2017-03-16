package mimir.algebra;

/**
 * Utility methods for manipulating expressions
 */
object ExpressionUtils {

	/**
	 * Extract the set of Var() terms (column references) in
	 * the specified expression
	 */
	def getColumns(e: Expression): Set[String] = 
  {
		e match {
			case Var(id) => Set(id)
			case _ => e.children.flatMap(getColumns(_)).toSet
		}
	}

  /**
   * Extract the set of Function() names referenced in the
   * specified expression
   */
  def getFunctions(e: Expression): Set[String] = 
  {
    e match {
      case Function(fn, args) => Set(fn) ++ args.flatMap(getFunctions(_)).toSet
      case _ => e.children.flatMap(getFunctions(_)).toSet
    }
  }

	/**
	 * Return true if the specified expression depends on
	 * data
	 */
	def isDataDependent(e: Expression): Boolean = {
		e match { 
			case Var(_) => true
			case RowIdVar() => true
			case _ => e.children.exists( isDataDependent(_) )
		}
	}

	/**
	 * SQL CASE expresions in form 1:
	 *   CASE WHEN A THEN X WHEN B THEN Y ELSE Z END
	 * This gets represented as:
	 *   ( [(A, X), (B, Y)], Z )
	 * This utility method converts a case expression into
	 * the equivalent chain of Mimir's If-Then-Else clauses:
	 * if(A){ X } else { if(B){ Y } else { Z } }
	 */
	def makeCaseExpression(
    whenThenClauses: List[(Expression, Expression)], 
		elseClause: Expression
  ): Expression =
	{
		whenThenClauses.foldRight(elseClause)( (wt, e) => 
			Conditional(wt._1, wt._2, e)
		)
	}

  /**
   * SQL CASE expresions in form 2:
   *   CASE Q WHEN A THEN X WHEN B THEN Y ELSE Z END
   * This gets represented as:
   *   ( Q, [(A, X), (B, Y)], Z )
   * This utility method converts a case expression into
   * the equivalent chain of Mimir's If-Then-Else clauses:
   * if(Q=A){ X } else { if(Q=B){ Y } else { Z } }
   */
  def makeCaseExpression(
    testClause: Expression,
    whenThenClauses: TraversableOnce[(Expression, Expression)], 
    elseClause: Expression
  ): Expression =
  {
    whenThenClauses.foldRight(elseClause)( (wt, e) => 
      Conditional(Comparison(Cmp.Eq, testClause, wt._1), wt._2, e)
    )
  }

  /** 
   * Inverse of makeCaseExpression.  For rewrite simplicity, Mimir 
   * uses If-Then-Else clauses.  To get back to SQL's CASE statements,
   * we need to find a specific pattern in the expression structure:
   *   if(A){ if(B) { W } else { X } } else { if(C){ Y } else { Z } }
   * becomes:
   *   CASE
   *     WHEN A AND B THEN W
   *     WHEN A THEN X
   *     WHEN C THEN Y
   *     ELSE Z
   *   END
   */
	def foldConditionalsToCase(e: Expression): 
		(Seq[(Expression, Expression)], Expression) =
	{
		foldConditionalsToCase(e, BoolPrimitive(true))
	}

	/**
	 * Utility method supporting foldConditionalsToCase
	 */
	private def foldConditionalsToCase(e: Expression, prefix: Expression): 
		(Seq[(Expression, Expression)], Expression) =
	{
		e match { 
			case Conditional(condition, thenClause, elseClause) =>
			{
				val thenCondition = makeAnd(prefix, condition)

				val (thenWhenThens, thenElse) = 
					foldConditionalsToCase(thenClause, thenCondition)

				val (elseWhenThens, elseElse) = 
					foldConditionalsToCase(elseClause, BoolPrimitive(true))
				(	
					thenWhenThens ++ Some((thenCondition, thenElse)) ++ elseWhenThens, 
					elseElse
				)
			}
			case _ => (List[(Expression, Expression)](), e)
		}
	}

  /**
   * Create an IN test (i.e., X IN (A, B, C, ...))
   */
  def makeInTest(lhs: Expression, cases: Seq[Expression]): Expression =
  {
    makeOr(cases.map( Comparison(Cmp.Eq, lhs, _) ))
  }

  /**
   * Create a sum from an arbitrary list
   */
  def makeSum(el: TraversableOnce[Expression]): Expression =
  {
    el.fold(IntPrimitive(0))(makeSum(_, _))
  }

  /**
   * Create a sum from two values, ignoring zeroes
   */
  def makeSum(a:Expression, b:Expression): Expression =
  {
    (a, b) match {
      case (IntPrimitive(0), _)     => b
      case (FloatPrimitive(0.0), _) => b
      case (_, IntPrimitive(0))     => a
      case (_, FloatPrimitive(0.0)) => a
      case _ => Arithmetic(Arith.Add, a, b)
    }
  }

  /**
   * Optimizing AND constructor that dynamically folds in 
   * Boolean constants.  For example:
   *   makeAnd(true, X) returns X
   */
  def makeAnd(a: Expression, b: Expression): Expression = 
    (a, b) match {
      case (BoolPrimitive(true), _) => b
      case (_, BoolPrimitive(true)) => a
      case (BoolPrimitive(false), _) => BoolPrimitive(false)
      case (_, BoolPrimitive(false)) => BoolPrimitive(false)
      case _ => Arithmetic(Arith.And, a, b)
    }
  /**
   * Optimizing AND constructor that dynamically folds in 
   * Boolean constants.  For example:
   *   makeAnd(true, X) returns X
   */
  def makeAnd(el: TraversableOnce[Expression]): Expression = 
    el.fold[Expression](BoolPrimitive(true))(makeAnd(_,_))
  /**
   * Optimizing OR constructor that dynamically folds in 
   * Boolean constants.  For example:
   *   makeOr(true, X) returns true
   */
  def makeOr(a: Expression, b: Expression): Expression = 
  (a, b) match {
    case (BoolPrimitive(false), _) => b
    case (_, BoolPrimitive(false)) => a
    case (BoolPrimitive(true), _) => BoolPrimitive(true)
    case (_, BoolPrimitive(true)) => BoolPrimitive(true)
    case _ => Arithmetic(Arith.Or, a, b)
  }
  /**
   * Optimizing OR constructor that dynamically folds in 
   * Boolean constants.  For example:
   *   makeOr(true, X) returns true
   */
  def makeOr(el: TraversableOnce[Expression]): Expression = 
    el.fold[Expression](BoolPrimitive(false))(makeOr(_,_))
  /**
   * Optimizing NOT constructor that dynamically folds in 
   * Boolean constants, ANDs and ORs using demorgan's laws,
   * and reverses terminals like comparators
   * For example:
   *   makeNot(X>2 AND true) returns X<=2
   */
  def makeNot(e: Expression): Expression =
  {
    e match {
      case BoolPrimitive(b) => BoolPrimitive(!b)
      case Arithmetic(Arith.And, a, b) =>
        makeOr(makeNot(a), makeNot(b))
      case Arithmetic(Arith.Or, a, b) =>
        makeAnd(makeNot(a), makeNot(b))
      case Comparison(c, a, b) =>
        Comparison(Cmp.negate(c), a, b)
      case Not(a) => a
      case _ => Not(e)
    }
  }
  /**
   * Flatten a hierarchy of AND operators into a list.
   * For example:
   *   AND(A, AND(AND(B, C), D) becomes [A, B, C, D]
   */
  def getConjuncts(e: Expression): Seq[Expression] = {
    e match {
      case BoolPrimitive(true) => List[Expression]()
      case Arithmetic(Arith.And, a, b) => 
        getConjuncts(a) ++ getConjuncts(b)
      case _ => List(e)
    }
  }
  /**
   * Flatten a hierarchy of OR operators into a list.
   * For example:
   *   OR(A, OR(OR(B, C), D) becomes [A, B, C, D]
   */
  def getDisjuncts(e: Expression): Seq[Expression] = {
    e match {
      case BoolPrimitive(false) => List[Expression]()
      case Arithmetic(Arith.Or, a, b) => 
        getConjuncts(a) ++ getConjuncts(b)
      case _ => List(e)
    }
  }

  /**
   * Cast an expression, but only if neceessary
   */
  def makeCast(e: Expression, targetT: Type, schema: Map[String,Type]): 
    Expression =
  {
    val sourceT = Typechecker.typeOf(e, schema)
    if(sourceT != targetT){
      Function("CAST", List(e, TypePrimitive(targetT)))
    } else {
      e
    }

  }
}