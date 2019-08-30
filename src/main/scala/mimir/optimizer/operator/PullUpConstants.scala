package mimir.optimizer.operator

import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir.algebra._
import mimir.optimizer.OperatorOptimization

object PullUpConstants extends OperatorOptimization with LazyLogging
{
  def apply(o:Operator): Operator =
  {
    val (rewritten, constants) = extractConstants(o)

    if(constants.isEmpty){ return rewritten }

    val expressionMappings = 
      rewritten.columnNames
               .map { col => col -> Var(col) }
               .toMap
    val bindings = expressionMappings ++ constants

    val projections = 
      o.columnNames
       .map { col => col -> bindings(col) }

    return rewritten.mapByID(projections:_*)
  }

  def extractConstants(o: Operator): 
    (Operator, Map[ID, PrimitiveValue]) =
  {
    logger.trace(s"EXTRACT CONSTANTS: $o")
    o match {
      case Project(columns, src) => {
        val (rewrittenSrc, bindings) = extractConstants(src)

        logger.trace(s"Project PULL UP : $o")

        val inlined:Seq[Either[ProjectArg, (ID, PrimitiveValue)]] = 
          columns.map { case ProjectArg(name, expr) =>
            name -> Eval.inline(expr, bindings)
          }.map { 
            case (col, p:PrimitiveValue) => Right(col -> p)
            case (col, e)  => Left(ProjectArg(col, e))
          }

        logger.trace(s"Project PULL UP Inlines : $inlined")

        ( 
          Project(inlined.collect { case Left(l) => l } , rewrittenSrc), 
          inlined.collect { case Right(r) => r }.toMap
        )
      }
      case Select(condition, src) => {
        val (rewrittenSrc, bindings) = extractConstants(src)

        (
          Select(Eval.inline(condition, bindings), rewrittenSrc),
          bindings
        )
      }
      case Aggregate(groupBy, aggregates, src) => {
        val (rewrittenSrc, bindings) = extractConstants(src)

        // First look at the group-by variables.  If any of them
        // are constants, they can be pulled out of the aggregate
        // entirely.
        val (constantGroupBy, variableGroupBy) = 
          groupBy.partition { v => bindings contains v.name }

        logger.trace(s"Aggregate PULL UP : $o")

        // next rewrite the aggregates
        val inlinedAggregates:Seq[Either[AggFunction, (ID, PrimitiveValue)]] = 
          aggregates.map { case AggFunction(name, distinct, args, alias) => 
            AggFunction(
              name, 
              distinct, 
              args.map { Eval.inline(_, bindings) },
              alias
            )
          }.map {
            case AggFunction(ID("group_and"), false, Seq(b:BoolPrimitive), alias) =>
              Right(alias -> b)
            case AggFunction(ID("group_or"), false, Seq(b:BoolPrimitive), alias) =>
              Right(alias -> b)
            case f => Left(f)
          }

        logger.trace(s"Aggregate Constant Groups : $constantGroupBy")
        logger.trace(s"Aggregate Inlines : $inlinedAggregates")

        (
          Aggregate(
            variableGroupBy,
            inlinedAggregates.collect { case Left(l) => l },
            rewrittenSrc
          ),
          constantGroupBy.map { v => v.name -> bindings(v.name) }.toMap ++
            inlinedAggregates.collect { case Right(r) => r }
        )
      }

      case Join(lhs, rhs) => {
        val (lhsRewritten, lhsBindings) = extractConstants(lhs)
        val (rhsRewritten, rhsBindings) = extractConstants(rhs)

        (
          Join(lhsRewritten, rhsRewritten),
          lhsBindings ++ rhsBindings
        )
      }

      case LeftOuterJoin(lhs, rhs, condition) => {
        val (lhsRewritten, lhsBindings) = extractConstants(lhs)
        val (rhsRewritten, rhsBindings) = extractConstants(rhs)
        val jointBindings = lhsBindings ++ rhsBindings

        (
          LeftOuterJoin(lhsRewritten, rhsRewritten, Eval.inline(condition, jointBindings)),
          jointBindings
        )
      }

      case Sort(sortCols, src) => {
        val (rewrittenSrc, bindings) = extractConstants(src)

        (
          Sort(
            sortCols.map { case SortColumn(expr, asc) => 
              SortColumn(Eval.inline(expr, bindings), asc)
            },
            rewrittenSrc
          ),
          bindings
        )
      }

      case Limit(offset, count, src) => {
        val (rewrittenSrc, bindings) = extractConstants(src)
        (
          Limit(offset, count, rewrittenSrc),
          bindings
        )
      }

      // Don't try to descend into any of the following.
      case _:LensView | _:View | _:HardTable | _:Table | _:Union  => (o, Map())

    }
  }

}