package mimir.ctables;

import mimir.algebra._;

class ColumnAnalysis(
    sources: List[(Expression, Operator)]
) {
  def getType: Type.T = {
    sources.map( _ match { case (x, o) => x.exprType(o.schema) } ).
            fold(Type.TAny)( Arith.escalateCompat(_,_) )
  }
  
  def bounds: (PrimitiveValue, PrimitiveValue) = {
    println("WARNING: ColumnAnalysis.bounds IS ONLY A PLACEHOLDER FOR NOW");
    (FloatPrimitive(0.0), FloatPrimitive(0.0))
  }
  
  def exprs = sources.map( _._1 )
}
  

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
  
  def analyze(col: String, oper: Operator): ColumnAnalysis =
    new ColumnAnalysis(expr(col, oper))
  
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
  	
  	def condition(oper: Operator): ColumnAnalysis =
  	{
    if(oper.schema.get(CTables.conditionColumn).isEmpty){ 
      new ColumnAnalysis(List[(Expression,Operator)](
          (BoolPrimitive(true), oper)
        ))
    } else {
      analyze(CTables.conditionColumn, oper)
    }
  }
  	

}