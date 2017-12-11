package mimir.optimizer.operator

import mimir.optimizer.OperatorOptimization
import mimir.algebra._
import mimir.algebra.function._

class EvaluateHardTables(typechecker: Typechecker, interpreter: Eval) extends OperatorOptimization
{
  def safeToEval(expr: Expression): Boolean =
  {
    expr match {
      case _:VGTerm => false
      case _        => expr.children.map { safeToEval(_) }.fold(true) { _ && _ }
    }
  }


  def apply(o: Operator): Operator =
  {

    o.recur(apply(_)) match { 
      case Union(
        HardTable(sch,lhs),
        HardTable(_,rhs)
      ) => HardTable(sch, lhs++rhs)

      case Join(
        HardTable(lhsSch,lhsData), 
        HardTable(rhsSch,rhsData)
      ) => HardTable(lhsSch++rhsSch, lhsData.flatMap { lhsRow => rhsData.map { lhsRow ++ _ }})

      case Select(cond, HardTable(sch,data)) 
        if safeToEval(cond) => 
      {
        val colNames = sch.map{ _._1 }
        HardTable(
          sch,
          data.filter { row => 
            interpreter.evalBool(cond, colNames.zip(row).toMap)
          }
        )
      }

      case Project(exprs, HardTable(sch, data)) 
        if exprs.forall { col => safeToEval(col.expression) } =>
      {
        val colNames = sch.map{ _._1 }
        val schMap = sch.toMap
        HardTable(
          exprs.map { col => (col.name, typechecker.typeOf(col.expression, schMap, Some(o))) },
          data.map { row => 
            val bindings = colNames.zip(row).toMap
            exprs.map { col =>
              interpreter.eval(col.expression, bindings)
            }
          }
        )
      }

      case somethingElse => somethingElse
    }
  }
}
