package mimir.algebra

trait OperatorConstructors
{
  def toOperator: Operator

  def union(other: Operator): Operator =
    Union(toOperator, other)

  def join(other: Operator, on: Expression = BoolPrimitive(true)): Operator =
    Join(toOperator, other).filter(on)

  def filter(condition: Expression): Operator =
    condition match {
      case BoolPrimitive(true)  => toOperator
      case BoolPrimitive(false) => EmptyTable(toOperator.schema)
      case _ => Select(condition, toOperator)
    }

  def project(cols: String*): Operator =
    map(cols.map { col => (col, Var(col)) }:_* )

  def map(cols: (String, Expression)*): Operator =
  {
    val (oldProjections, strippedOperator) =
      OperatorUtils.extractProjections(toOperator)
    val lookupMap: Map[String,Expression] = 
      oldProjections
        .map { _.toBinding }
        .toMap

    Project(
      cols.map { case (col, expr) => 
        ProjectArg(col, Eval.inline(expr, lookupMap))
      },
      strippedOperator
    )
  }

  def removeColumn(targets: String*): Operator =
  {
    val isTarget = targets.toSet
    val (cols, src) = OperatorUtils.extractProjections(toOperator)
    Project(
      cols.filterNot { col => isTarget(col.name.toUpperCase) },
      src
    )
  }

  def addColumn(newCols: (String, Expression)*): Operator =
  {
    val (cols, src) = OperatorUtils.extractProjections(toOperator)
    Project(
      cols ++ newCols.map { col => ProjectArg(col._1, col._2) },
      src
    )
  }

  def distinct: Operator =
  {
    val base = toOperator
    Aggregate(
      base.columnNames.map { Var(_) },
      Seq(),
      base
    )
  }

  def count(distinct: Boolean = false, alias: String = "COUNT"): Operator =
  {
    Aggregate(
      Seq(), 
      Seq(AggFunction("COUNT", distinct, Seq(), alias)), 
      toOperator
    )
  }

  def sort(sortCols: (String, Boolean)*): Operator =
    Sort( sortCols.map { col => SortColumn(Var(col._1), col._2) }, toOperator )

  def limit(count: Int = -1, offset: Int = 0 ): Operator =
    Limit( offset, if(count >= 0) { Some(count) } else { None }, toOperator )
}