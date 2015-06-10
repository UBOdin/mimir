package mimir.algebra;

import mimir.algebra.Type._
import mimir.ctables.CTables

object Eval 
{

  /**
   * Evaluate the specified expression and cast the result to an Long
   */
  def evalInt(e: Expression) =
    eval(e).asLong
  /**
   * Evaluate the specified expression and cast the result to a String
   */
  def evalString(e: Expression) =
    eval(e).asString
  /**
   * Evaluate the specified expression and cast the result to a Double
   */
  def evalFloat(e: Expression) =
    eval(e).asDouble
  /**
   * Evaluate the specified expression and cast the result to a Boolean
   */
  def evalBool(e: Expression): Boolean =
    eval(e) match {
      case BoolPrimitive(v) => v
      case v => throw new TypeException(TBool, v.exprType, "Cast")
    }
  /**
   * Evaluate the specified expression and return the primitive value
   */
  def eval(e: Expression): PrimitiveValue = 
    eval(e, Map[String, PrimitiveValue]());

  /**
   * Evaluate the specified expression given a set of Var/Value bindings
   * and return the primitive value of the result
   */
  def eval(e: Expression, 
           bindings: Map[String, PrimitiveValue]
  ): PrimitiveValue = 
  {
    if(e.isInstanceOf[PrimitiveValue]){
      return e.asInstanceOf[PrimitiveValue]
    } else {
      e match {
        case Var(v) => bindings.get(v).get
        case Arithmetic(op, lhs, rhs) =>
          applyArith(op, eval(lhs, bindings), eval(rhs, bindings))
        case Comparison(op, lhs, rhs) =>
          applyCmp(op, eval(lhs, bindings), eval(rhs, bindings))
        case CaseExpression(caseWhens, caseElse) =>
          caseWhens.foldLeft(None: Option[PrimitiveValue])( (a, b) =>
            if(a != None){ a }
            else {
              if(eval(b.when, bindings).
                    asInstanceOf[BoolPrimitive].v){
                Some(eval(b.then, bindings));
              } else { None }
            }
          ).getOrElse(eval(caseElse, bindings))
        case Not(NullPrimitive()) => NullPrimitive()
        case Not(e) => BoolPrimitive(!evalBool(e))
        case p:Proc => p.get(p.getArgs.map(eval(_, bindings)))
        case IsNullExpression(c, n) => {
          val isNull: Boolean = 
            eval(c, bindings).
            isInstanceOf[NullPrimitive];
          if(n){ return BoolPrimitive(!isNull); }
          else { return BoolPrimitive(isNull); }
        }
      }
      
    }
  }

  /**
   * Apply one level of simplification to the passed expression.  Typically
   * this method should not be used directly, but is invoked as part of
   * either inline() method.
   *
   * If the expression is independent of VGTerms or variable references
   * then the expression can be deterministically evaluated outright.
   * In this case, simplify() returns the PrimitiveValue that the 
   * expression evaluates to.  
   *
   * CASE statements are simplified further.  See simplifyCase()
   */
  def simplify(e: Expression): Expression = {
    if(getVars(e).isEmpty && 
       !CTables.isProbabilistic(e)) 
    { 
      eval(e) 
    } else e match { 
      case CaseExpression(wtClauses, eClause) =>
        simplifyCase(List(), wtClauses, eClause)
      case _ => e
    }
  }

  /**
   * Recursive method that optimizes case statements.
   *
   * - WHEN clauses that are deterministically false are dropped from 
   *   the case expression. 
   * - WHEN clauses that are deterministically true become else clauses
   *   and all following WHEN and ELSE clauses are dropped.
   * - If the ELSE clause is the only remaining clause in the expression,
   *   it replaces the entire CASE expression.
   */
  def simplifyCase(wtSimplified: List[WhenThenClause], 
                   wtTodo: List[WhenThenClause], 
                   eClause: Expression): Expression =
    wtTodo match {
      case WhenThenClause(w, t) :: wtRest =>
        if(w.isInstanceOf[BoolPrimitive]){
          if(w.asInstanceOf[BoolPrimitive].v){

            // If the when condition is deterministically true, then
            // we can turn the current then statement into an else
            // branch and finish here.  For the sake of keeping all
            // of the reconstruction code in one place, we recur into
            // a terminal leaf.
            simplifyCase(wtSimplified, List(), t)
          } else {

            // If the when condition is deterministically false, then
            // we strip the current clause out of the todo list and
            // recur as normal.
            simplifyCase(wtSimplified, wtRest, eClause)
          }
        } else {

          // If the when condition is neither deterministically true,
          // nor false, we add it on the "finished" list and then recur
          // as normal.
          simplifyCase(wtSimplified ++ List(WhenThenClause(w,t)), 
                       wtRest, eClause)
        }

      case _ => // empty list

        // If none of the when clauses can possibly be triggered, we
        // always fall through to the else clause.
        if(wtSimplified.isEmpty){ eClause }
        else {
          //otherwise, we rebuild the case statement
          CaseExpression(wtSimplified, eClause)
        }
    }

  /**
   * Thoroughly inline an expression, recursively applying simplify()
   * at levels, to all subtrees of the expression.
   */
  def inline(e: Expression): Expression = 
    inline(e, Map[String, Expression]())
  /**
   * Apply a given variable binding to the specified expression, and then
   * thoroughly inline it, recursively applying simplify() at all levels,
   * to all subtrees of the expression
   */
  def inline(e: Expression, bindings: Map[String, Expression]):
    Expression = 
  {
    e match {
      case Var(v) => bindings.get(v).getOrElse(Var(v))
      case _ => 
        simplify( e.rebuild( e.children.map( inline(_, bindings) ) ) )

    }
  }
  
  /**
   * Perform arithmetic on two primitive values.
   */
  def applyArith(op: Arith.Op, 
            a: PrimitiveValue, b: PrimitiveValue
  ): PrimitiveValue = {
    if(a.isInstanceOf[NullPrimitive] || 
       b.isInstanceOf[NullPrimitive]){
      NullPrimitive()
    } else {
      (op, Arith.computeType(op, a.exprType, b.exprType)) match { 
        case (Arith.Add, TInt) => 
          IntPrimitive(a.asLong + b.asLong)
        case (Arith.Add, TFloat) => 
          FloatPrimitive(a.asDouble + b.asDouble)
        case (Arith.Sub, TInt) => 
          IntPrimitive(a.asLong - b.asLong)
        case (Arith.Sub, TFloat) => 
          FloatPrimitive(a.asDouble - b.asDouble)
        case (Arith.Mult, TInt) => 
          IntPrimitive(a.asLong * b.asLong)
        case (Arith.Mult, TFloat) => 
          FloatPrimitive(a.asDouble * b.asDouble)
        case (Arith.Div, (TFloat|TInt)) => 
          FloatPrimitive(a.asDouble / b.asDouble)
        case (Arith.And, TBool) => 
          BoolPrimitive(
            a.asInstanceOf[BoolPrimitive].v &&
            b.asInstanceOf[BoolPrimitive].v
          )
        case (Arith.Or, TBool) => 
          BoolPrimitive(
            a.asInstanceOf[BoolPrimitive].v ||
            b.asInstanceOf[BoolPrimitive].v
          )
      }
    }
  }

  /**
   * Perform a comparison on two primitive values.
   */
  def applyCmp(op: Cmp.Op, 
            a: PrimitiveValue, b: PrimitiveValue
  ): PrimitiveValue = {
    if(a.isInstanceOf[NullPrimitive] || 
       b.isInstanceOf[NullPrimitive]){
      NullPrimitive()
    } else {
      Cmp.computeType(op, a.exprType, b.exprType)
      op match { 
        case Cmp.Eq => 
          BoolPrimitive(a.payload.equals(b.payload))
        case Cmp.Neq => 
          BoolPrimitive(!a.payload.equals(b.payload))
        case Cmp.Gt => 
          Arith.escalateNumeric(a.exprType, b.exprType) match {
            case TInt => BoolPrimitive(a.asLong > b.asLong)
            case TFloat => BoolPrimitive(a.asDouble > b.asDouble)
            case TDate => 
              BoolPrimitive(
                a.asInstanceOf[DatePrimitive].
                 compare(b.asInstanceOf[DatePrimitive])<0
              )
          }
        case Cmp.Gte => 
          Arith.escalateNumeric(a.exprType, b.exprType) match {
            case TInt => BoolPrimitive(a.asLong >= b.asLong)
            case TFloat => BoolPrimitive(a.asDouble >= b.asDouble)
            case TDate => 
              BoolPrimitive(
                a.asInstanceOf[DatePrimitive].
                 compare(b.asInstanceOf[DatePrimitive])<=0
              )
          }
        case Cmp.Lt => 
          Arith.escalateNumeric(a.exprType, b.exprType) match {
            case TInt => BoolPrimitive(a.asLong < b.asLong)
            case TFloat => BoolPrimitive(a.asDouble < b.asDouble)
            case TDate => 
              BoolPrimitive(
                a.asInstanceOf[DatePrimitive].
                 compare(b.asInstanceOf[DatePrimitive])>0
              )
          }
        case Cmp.Lte => 
          Arith.escalateNumeric(a.exprType, b.exprType) match {
            case TInt => BoolPrimitive(a.asLong <= b.asLong)
            case TFloat => BoolPrimitive(a.asDouble <= b.asDouble)
            case TDate => 
              BoolPrimitive(
                a.asInstanceOf[DatePrimitive].
                 compare(b.asInstanceOf[DatePrimitive])>=0
              )
          }
      }
    }
  }

  /**
   * Compute the set of variables that appear in the specified expression.
   */
  def getVars(e: Expression): Set[String] = 
    e match { 
      case Var(v) => Set(v)
      case _ => e.children.map(getVars(_)).fold(Set[String]())( _ ++ _ )
    }
}