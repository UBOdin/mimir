package mimir.optimizer.operator

import com.typesafe.scalalogging.LazyLogging
import mimir.optimizer.OperatorOptimization
import mimir.algebra._
import mimir.algebra.function._

class EvaluateHardTables(typechecker: Typechecker, interpreter: Eval) 
  extends OperatorOptimization
  with LazyLogging
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
        val newData =           data.filter { row => 
          val bindings = colNames.zip(row).toMap
          val ret = interpreter.evalBool(cond, bindings)
          logger.trace(s"$bindings ::> $ret")
          ret
        }
        logger.debug(s"SELECT WHERE $cond : ${data.size} -> ${newData.size} rows")

        HardTable(sch, newData)
      }

      case Project(exprs, HardTable(Seq(), Seq())) 
        if exprs.forall { col => safeToEval(col.expression) } =>
      {
        val sch = typechecker.schemaOf(o).toMap
        HardTable(
          exprs.map { col => (col.name, sch(col.name))},
          Seq(exprs.map { col =>
            interpreter.eval(col.expression)
          })
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
