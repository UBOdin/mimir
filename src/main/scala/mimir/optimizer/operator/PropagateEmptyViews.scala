package mimir.optimizer.operator

import mimir.optimizer.OperatorOptimization
import mimir.algebra._
import mimir.algebra.function._

class PropagateEmptyViews(typechecker: Typechecker, aggregates: AggregateRegistry) extends OperatorOptimization
{
  def apply(o: Operator): Operator =
  {
    o.recur(apply(_)) match { 
      case Union(HardTable(sch,Seq()), x)          => x
      case Union(x, HardTable(sch,Seq()))          => x

      case Join(HardTable(sch,Seq()), _)           => HardTable(typechecker.schemaOf(o),Seq())
      case Join(_, HardTable(sch,Seq()))           => HardTable(typechecker.schemaOf(o),Seq())

      case Select(BoolPrimitive(false), _)  => HardTable(typechecker.schemaOf(o),Seq())
      case Select(_, HardTable(sch,Seq()))       => HardTable(sch,Seq())
      case Project(_, HardTable(sch,Seq()))      => HardTable(typechecker.schemaOf(o),Seq())
      case Sort(_, HardTable(sch,Seq()))         => HardTable(sch,Seq())
      case Limit(_, _, HardTable(sch,Seq()))     => HardTable(sch,Seq())

      case Aggregate(Seq(), agg, HardTable(sch,Seq())) => {
        val schMap = sch.toMap
        val (nsch,data) = agg.map { 
          case AggFunction(function, _, args, alias) => 
            (
              (
                alias,
                aggregates.typecheck(
                  function, 
                  args.map { expr => 
                    typechecker.typeOf(expr,schMap)
                  }
                )
              ), 
              aggregates.defaultValue(function)
            ) 
          }.unzip
        HardTable(nsch, Seq(data))
      }
      case Aggregate(_, _, HardTable(sch,Seq())) => HardTable(typechecker.schemaOf(o),Seq())

      case x => x
    }
  }
}