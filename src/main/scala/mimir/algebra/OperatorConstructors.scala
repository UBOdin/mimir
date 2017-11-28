package mimir.algebra

import mimir.parser.ExpressionParser

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
      case _ => Select(condition, toOperator)
    }
  def filterParsed(condition: String): Operator =
    filter(ExpressionParser.expr(condition))

  def project(cols: String*): Operator =
    mapImpl(cols.map { col => (col, Var(col)) } )

  def projectNoInline(cols: String*): Operator =
    mapImpl(cols.map { col => (col, Var(col)) }, noInline = true)

  def mapParsed(cols: (String, String)*): Operator =
    mapImpl(cols.map { case (name, expr) => (name, ExpressionParser.expr(expr)) } )

  def map(cols: (String, Expression)*): Operator =
    mapImpl(cols)

  def mapParsedNoInline(cols: (String, String)*): Operator =
    mapImpl(
      cols.map { case (name, expr) => (name, ExpressionParser.expr(expr)) },
      noInline = true
    )

  def mapNoInline(cols: (String, Expression)*): Operator =
    mapImpl(cols, noInline = true)

  def mapImpl(cols: Seq[(String, Expression)], noInline:Boolean = false): Operator =
  {
    val (oldProjections, strippedOperator) =
      if(noInline) { 
        (toOperator.columnNames.map { x => ProjectArg(x, Var(x)) }, toOperator)
      } else {
        OperatorUtils.extractProjections(toOperator)
      }
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

  def rename(targets: (String, String)*): Operator =
  {
    val renamings = targets.toMap
    map(toOperator.columnNames.map { 
      case name if renamings contains name => (renamings(name), Var(name)) 
      case name => (name, Var(name))
    }:_*)
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
    val bindings = cols.map { _.toBinding }.toMap
    Project(
      cols ++ newCols.map { col => ProjectArg(col._1, Eval.inline(col._2, bindings)) },
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

  def aggregateParsed(agg: (String, String)*): Operator =
    groupByParsed()(agg:_*)

  def aggregate(agg: AggFunction*): Operator =
    groupBy()(agg:_*)

  def groupByParsed(gb: String*)(agg: (String, String)*): Operator =
    groupBy(gb.map { Var(_) }:_*)(
      agg.map { case (alias, fnExpr) => 
        val fn = ExpressionParser.function(fnExpr)
        AggFunction(fn.op, false, fn.params, alias)
      }:_*)

  def groupBy(gb: Var*)(agg: AggFunction*): Operator =
  {
    Aggregate(gb, agg, toOperator)
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