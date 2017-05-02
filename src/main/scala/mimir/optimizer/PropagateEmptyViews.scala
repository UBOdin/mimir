package mimir.optimizer

import mimir.algebra._

object PropagateEmptyViews 
{
  def apply(o: Operator): Operator =
  {
    o.recur(apply(_)) match { 
      case Union(EmptyTable(_), x)          => x
      case Union(x, EmptyTable(_))          => x

      case Join(EmptyTable(_), _)           => EmptyTable(o.schema)
      case Join(_, EmptyTable(_))           => EmptyTable(o.schema)

      case Select(cond, _) 
        if Eval.simplify(cond).equals(BoolPrimitive(false)) 
                                            => EmptyTable(o.schema)
      case Select(_, EmptyTable(sch))       => EmptyTable(sch)
      case Project(_, EmptyTable(sch))      => EmptyTable(sch)
      case Sort(_, EmptyTable(sch))         => EmptyTable(sch)
      case Limit(_, _, EmptyTable(sch))     => EmptyTable(sch)
      case Aggregate(_, _, EmptyTable(sch)) => EmptyTable(o.schema)

      case x => x
    }
  }
}