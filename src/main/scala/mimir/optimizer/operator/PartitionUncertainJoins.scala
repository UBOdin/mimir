package mimir.optimizer.operator

import com.typesafe.scalalogging.LazyLogging
import mimir.algebra._
import mimir.ctables._
import mimir.optimizer.OperatorOptimization

object PartitionUncertainJoins extends OperatorOptimization with LazyLogging
{
  def containsDataDependentVGTerm(e: Expression): Boolean =
  {
    CTables.getVGTerms(e).exists { _.isDataDependent }
  }

  def isGoodJoin(cond: Expression, lSrc: Operator, rSrc: Operator): Boolean =
  {
    val lNames = lSrc.columnNames.toSet
    val rNames = rSrc.columnNames.toSet
    logger.debug(s"Checking for good join: $cond in $lNames <-> $rNames")
    cond match {
      case Comparison(Cmp.Eq, a, b) =>
        val aCols = ExpressionUtils.getColumns(a)
        val bCols = ExpressionUtils.getColumns(b)
        return (
             containsDataDependentVGTerm(a) 
          || containsDataDependentVGTerm(b)
        ) && (
          (aCols.forall { lNames(_) } && bCols.forall { rNames(_) }) 
        ||(bCols.forall { lNames(_) } && aCols.forall { rNames(_) }) 
        )
      case _ => return false
    }
  }

  def split(predicate: Expression, join: Join): Seq[Expression] =
  {
    predicate match {
      case cond: Conditional =>
      {
        logger.debug(s"Found a potential candidate: $cond @ \n$join")
        if(isGoodJoin(cond.thenClause, join.left, join.right) || 
           isGoodJoin(cond.elseClause, join.left, join.right)) 
        {
          Seq(
            ExpressionUtils.makeAnd(cond.condition, cond.thenClause), 
            ExpressionUtils.makeAnd(ExpressionUtils.makeNot(cond.condition), cond.elseClause)
          )
        } else {
          Seq(cond)
        }
      }

      case Arithmetic(Arith.And, lhs, rhs) => 
        val rhsSplit = split(rhs, join)
        split(lhs, join).flatMap { lhsElement => rhsSplit.map { ExpressionUtils.makeAnd(lhsElement, _) } }

      case _ => Seq(predicate)
    }
  }

  def apply(o: Operator): Operator =
  {
    o match {
      case Select(predicate, join: Join) =>
      {
        val rewrittenJoin = join.recur(apply(_))
        OperatorUtils.makeUnion(
          split(predicate, join).map {
            Select(_, rewrittenJoin)
          }
        )
      }

      case _ => 
        o.recur(apply(_))
    }
  }
}