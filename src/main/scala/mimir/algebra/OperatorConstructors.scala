package mimir.algebra

import mimir.parser.ExpressionParser

trait OperatorConstructors
{
  def toOperator: Operator

  private def resolve(col: String):Var = 
    Var(toOperator.resolveCaseInsensitiveColumn(col).get)

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

  def projectByID(cols: ID*): Operator =
    mapImpl(cols.map { col => (col, Var(col)) } )
  def project(cols: String*): Operator =
    mapImpl(cols.map { col => (ID(col), resolve(col)) } )

  def projectNoInlineByID(cols: ID*): Operator =
    mapImpl(cols.map { col => (col, Var(col)) }, noInline = true)
  def projectNoInline(cols: String*): Operator =
    mapImpl(cols.map { col => (ID(col), resolve(col)) }, noInline = true)

  def mapParsed(cols: (String, String)*): Operator =
    mapImpl(cols.map { case (name, expr) => (ID(name), ExpressionParser.expr(expr)) } )

  def map(cols: (String, Expression)*): Operator =
    mapImpl(cols.map { case (name, expr) => (ID(name), expr) })
  def mapByID(cols: (ID, Expression)*): Operator =
    mapImpl(cols.map { case (name, expr) => (name, expr) })

  def mapParsedNoInline(cols: (String, String)*): Operator =
    mapImpl(
      cols.map { case (name, expr) => (ID(name), ExpressionParser.expr(expr)) },
      noInline = true
    )

  def mapNoInline(cols: (String, Expression)*): Operator =
    mapImpl(cols.map { case (name, expr) => (ID(name), expr) }, noInline = true)

  def mapImpl(cols: Seq[(ID, Expression)], noInline:Boolean = false): Operator =
  {
    val (oldProjections, strippedOperator) =
      if(noInline) { 
        (toOperator.columnNames.map { x => ProjectArg(x, Var(x)) }, toOperator)
      } else {
        OperatorUtils.extractProjections(toOperator)
      }
    val lookupMap: Map[ID,Expression] = 
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
    renameByID( targets.map { case (s, t) => resolve(s).name -> ID(t) }:_* )
  def renameByID(targets: (ID, ID)*): Operator =
  {
    val renamings = targets.toMap
    mapImpl(toOperator.columnNames.map { 
      case name if renamings contains name => (renamings(name), Var(name)) 
      case name => (name, Var(name))
    })
  }

  def removeColumns(targets: String*): Operator =
    removeColumnsByID( targets.map { resolve(_).name }:_* )
  def removeColumnsByID(targets: ID*): Operator =
  {
    val isTarget = targets.toSet
    val (cols, src) = OperatorUtils.extractProjections(toOperator)
    Project(
      cols.filterNot { col => isTarget(col.name) },
      src
    )
  }

  def addColumns(newCols: (String, Expression)*): Operator =
    addColumnsByID(newCols.map { case (col, expr) => resolve(col).name -> expr }:_*)
  def addColumnsByID(newCols: (ID, Expression)*): Operator =
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
    groupBy(gb.map { resolve(_) }:_*)(
        agg.map( { case (alias, fnExpr) => 
          {
            val fn = ExpressionParser.function(fnExpr)
            AggFunction(fn.op, false, fn.params, ID(alias))
          }
        }):_*
      )

  def groupBy(gb: Var*)(agg: AggFunction*): Operator =
  {
    Aggregate(gb, agg, toOperator)
  }


  def count(distinct: Boolean = false, alias: String = "COUNT"): Operator =
  {
    Aggregate(
      Seq(), 
      Seq(AggFunction(ID("COUNT"), distinct, Seq(), ID(alias))), 
      toOperator
    )
  }

  def countIf(alias: String = "COUNT")(condition: Expression): Operator =
  {
    Aggregate(
      Seq(),
      Seq(
        AggFunction(
          ID("SUM"), 
          false, 
          Seq(condition.thenElse { IntPrimitive(1) } { IntPrimitive(0) }),
          ID(alias)
        )
      ),
      toOperator
    )
  }

  def pctIf(alias: String = "FRACTION")(condition: Expression): Operator =
  {
    Aggregate(
      Seq(),
      Seq(
        AggFunction(
          ID("AVG"), 
          false, 
          Seq(condition.thenElse { FloatPrimitive(1) } { FloatPrimitive(0) }),
          ID(alias)
        )
      ),
      toOperator
    )
  }

  def sort(sortCols: (String, Boolean)*): Operator =
    sortByVar( sortCols.map { x => resolve(x._1) -> x._2 }:_* )
  def sortByID(sortCols: (ID, Boolean)*): Operator =
    sortByVar( sortCols.map { x => Var(x._1) -> x._2 }:_* )
  def sortByVar(sortCols: (Var, Boolean)*): Operator =
    Sort( sortCols.map { col => SortColumn(col._1, col._2) }, toOperator )

  def limit(count: Int = -1, offset: Int = 0 ): Operator =
    Limit( offset, if(count >= 0) { Some(count) } else { None }, toOperator )
}