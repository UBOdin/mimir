package mimir.ctables;

import mimir.algebra._;
import mimir.Database;

object CTAnalysis {
  	
  def isProbabilistic(expr: Expression): Boolean = 
  expr match {
  	case PVar(_, _, _, _) => true;
  	case _ => expr.children.exists( isProbabilistic(_) )
  }
  
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
  
  def basis(oper: Operator): Operator = 
  {
    oper match {
      case Project(cols, s) => s
      case _ => oper
    }
  }
    
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
}

abstract class CTAnalysis(db: Database) {
  def varType: Type.T
  def isCategorical: Boolean
  def computeMLE(element: List[PrimitiveValue]): PrimitiveValue
  def computeEqConfidence(element: List[PrimitiveValue], value: PrimitiveValue): Double
  def computeBounds(element: List[PrimitiveValue]): (Double,Double)
  def computeStdDev(element: List[PrimitiveValue]): Double
}