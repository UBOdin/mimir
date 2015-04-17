package mimir.ctables;

import mimir.algebra._;
import mimir.Database;
import java.sql.SQLException;

object CTAnalysis {
  
  /**
   * Could the provided Expression be probabilistic?
   * 
   * Returns true if there is a PVar anywhere in it
   */
  def isProbabilistic(expr: Expression): Boolean = 
  expr match {
  	case PVar(_, _, _, _) => true;
  	case _ => expr.children.exists( isProbabilistic(_) )
  }
  
  /**
   * Could the provided Operator be probabilistic.  
   *
   * Returns true if there is a PVar referenced
   * by the provided Operator or its children
   */
  def isProbabilistic(oper: Operator): Boolean = 
  {
  	(oper match {
  	  case Project(cols, _) => 
  	    cols.exists( (x: ProjectArg) => isProbabilistic(x.input) )
  	  case Select(expr, _) => 
  	    isProbabilistic(expr)
  	  case _ => false;
  	}) || oper.children.exists( isProbabilistic(_) )
  }
  
  /**
   * Strip a projection off of the top of an operator tree
   */
  def basis(oper: Operator): Operator = 
  {
    oper match {
      case Project(cols, s) => s
      case _ => oper
    }
  }
    
  /** 
   * Strip the expression required to compute a single column out
   * of the provided operator tree.  
   * Essentially a shortcut for an optimized form of 
   *
   * Project( [Var(col)], oper )
   * 
   * is equivalent to (and will often be slower than): 
   *
   * Project(ret(0)._1, ret(0)._2) UNION 
   *     Project(ret(1)._1, ret(1)._2) UNION
   *     ...
   *     Project(ret(N)._1, ret(N)._2) UNION
   */
  def expr(col: String, oper: Operator): 
    List[(Expression, Operator)] =
  {
    oper match {
      case p @ Project(_, src) => 
        List[(Expression,Operator)]((p.bindings.get(col).get, src))
      case Union(lhs, rhs) =>
        expr(col, lhs) ++ expr(col, rhs)
      case _ => 
        List[(Expression,Operator)]((Var(col), oper))
    }
  }

  
  /**
   * Construct a boolean expression that evaluates whether the input 
   * expression is deterministic <b>for a given input row</b>.  
   * Note the corner-case here: CASE (aka branch) statements can
   * create some tuples that are deterministic and others that are
   * non-deterministic (depending on which branch is taken).  
   *
   * Base cases: <ul>
   *   <li>pVar is false</li>
   *   <li>Var | Const is true</li>
   * </ul>
   * 
   * Everything else (other than CASE) is an AND of whether the 
   * child subexpressions are deterministic
   */
  def compileDeterministic(expr: Expression): Expression =
  {
    if(!CTAnalysis.isProbabilistic(expr)){
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
      
      case pvar: PVar =>
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

  def compileForBounds(
      expr: Expression, 
      procHandler: (Proc => (Expression, Expression))
    ): (Expression,Expression) =
  {
    val recur = ( (x:Expression) => compileForBounds(x,procHandler) );
    if(!isProbabilistic(expr)){ return (expr,expr); }
    expr match {
      case Not(child) => recur(child) match { case (a,b) => (b,a) }

      case (proc:Proc) => procHandler(proc)

      case Arithmetic(op, lhs, rhs) =>
        val (lhs_low, lhs_high) = recur(lhs);
        val (rhs_low, rhs_high) = recur(rhs);

        op match {
          case Arith.Add =>
            return (Arithmetic(Arith.Add, lhs_low , rhs_low ), 
                    Arithmetic(Arith.Add, lhs_high, rhs_high));

          case Arith.Sub => 
            return (Arithmetic(Arith.Sub, lhs_low , rhs_high),
                    Arithmetic(Arith.Sub, lhs_high, rhs_low ));

          case Arith.Mult =>
            // 
            // If a < 0 and b < c
            // then a * b > a * c (multiply Eq 2 by a negative number)
            //
            // Possible cases:
            //   lhs_low < 0 and rhs_low < 0
            //     min of : lhs_low * rhs_high (lhs_low * rhs_low > lhs_low * rhs_high)
            //         and  
            //   lhs_low > 0
            //     multip


            val low_options = List(
                Arithmetic(Arith.Mult, lhs_low, rhs_low),
                Arithmetic(Arith.Mult, lhs_high, rhs_high),
              )

            return (CaseExpression(
                List[WhenThenClause(
                  Arithmetic(Arith.Or, 
                    Arithmetic(Arith.And, 
                      Comparison(Cmp.Lte, lhs_low, IntPrimitive(0)),
                      Comparison(Cmp.Lte, lhs_low, IntPrimitive(0)),
                    )
              )

              )

            return (Arithmetic(Arith.Mult, lhs_low , rhs_high),
                    Arithmetic(Arith.Mult, lhs_high, rhs_low ));

        }


      case _ => assert(false); (BoolPrimitive(false),BoolPrimitive(false))

    }
  }

}

abstract class CTAnalysis(db: Database) {
  def varType: Type.T
  def isCategorical: Boolean
  def computeMLE(element: List[PrimitiveValue]): PrimitiveValue
  def computeEqConfidence(element: List[PrimitiveValue], value: PrimitiveValue): Double
  def computeBounds(element: List[PrimitiveValue]): (Double,Double)
  def computeStdDev(element: List[PrimitiveValue]): Double
}

class AnalysisMLEProjection(analysis: CTAnalysis, args: List[Expression])
  extends Proc(args)
{
  def exprType(bindings: Map[String,Type.T]) = analysis.varType;
  def rebuild(x: List[Expression]) = new AnalysisMLEProjection(analysis, x);
  def get() = analysis.computeMLE(args.map(Eval.eval(_)));
}