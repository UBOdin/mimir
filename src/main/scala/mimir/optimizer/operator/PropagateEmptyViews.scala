package mimir.optimizer.operator

import mimir.optimizer.OperatorOptimization
import mimir.algebra._
import mimir.algebra.function._

class PropagateEmptyViews(typechecker: Typechecker, aggregates: AggregateRegistry) extends OperatorOptimization
{
  def apply(o: Operator): Operator =
  {
    o.recur(apply(_)) match { 
      case Union(EmptyTable(_), x)          => x
      case Union(x, EmptyTable(_))          => x

      case Join(EmptyTable(_), _)           => EmptyTable(typechecker.schemaOf(o))
      case Join(_, EmptyTable(_))           => EmptyTable(typechecker.schemaOf(o))

      case Select(BoolPrimitive(false), _)  => EmptyTable(typechecker.schemaOf(o))
      case Select(_, EmptyTable(sch))       => EmptyTable(sch)
      case Project(_, EmptyTable(sch))      => EmptyTable(typechecker.schemaOf(o))
      case Sort(_, EmptyTable(sch))         => EmptyTable(sch)
      case Limit(_, _, EmptyTable(sch))     => EmptyTable(sch)

      case Aggregate(Seq(), agg, EmptyTable(sch)) => {
        SingletonTable(
          agg.map { case AggFunction(function, _, _, alias) => (alias, aggregates.defaultValue(function)) }
        )
      }
      case Aggregate(_, _, EmptyTable(sch)) => EmptyTable(typechecker.schemaOf(o))

      case x => x
    }
  }
}