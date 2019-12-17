package mimir.exec.shortcut

import mimir.Database
import mimir.algebra._
import mimir.exec.EvalInlined
import mimir.exec.result._

class SortOrdering(
  expr: ((Row, Row)) => Long
) 
  extends Ordering[Row]
{
  def compare(x: Row, y: Row) = expr(x,  y).toInt
}

object SortOrdering
{
  /**
   * Produce a comparator function 
   * 
   * -1: lhs is smaller than rhs
   * 0 : lhs == rhs
   * +1: lhs is bigger than rhs
   */
  def apply(db: Database, sortColumns: Seq[SortColumn], source: ResultIterator): SortOrdering =
  {
    new SortOrdering(
      makeEval(db, source)
        .compileForLong(compileSort(sortColumns))
    )
  }

  def prefixVars(expr: Expression, prefix: String): Expression =
  {
    expr match {
      case Var(v) => Var(ID(prefix, v))
      case _ => expr.recur(prefixVars(_, prefix))
    }
  }

  def compileSort(sortColumns: Seq[SortColumn]): Expression =
  {
    if(sortColumns.isEmpty){ return IntPrimitive(0) }
    else {
      val sort = sortColumns.head
      val rest = compileSort(sortColumns.tail)

      val lhsExpr = prefixVars(sort.expression, "LHS_")
      val rhsExpr = prefixVars(sort.expression, "RHS_")

      Conditional(
        lhsExpr gt rhsExpr,
        IntPrimitive( if(sort.ascending) { 1 } else { -1 } ),
        Conditional(
          rhsExpr gt lhsExpr,
          IntPrimitive( if(sort.ascending) { -1 } else { 1 } ),
          rest
        )
      )
    }
  }

  def makeEval(db: Database, source:ResultIterator): EvalInlined[(Row, Row)] =
  {
    new EvalInlined(
      (
        source.tupleSchema
              .zipWithIndex
              .map { 
                case ((col, t), i) => ( ID("LHS_", col) -> (t, (x:(Row, Row)) => x._1(i)) )
              } ++
        source.tupleSchema
              .zipWithIndex
              .map { 
                case ((col, t), i) => ( ID("RHS_", col) -> (t, (x:(Row, Row)) => x._2(i)) )
              }
      ).toMap,
      db
    )
  }
}