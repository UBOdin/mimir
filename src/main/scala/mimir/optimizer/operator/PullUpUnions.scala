package mimir.optimizer.operator

import com.typesafe.scalalogging.slf4j.LazyLogging
import mimir.algebra._
import mimir.optimizer.OperatorOptimization

class PullUpUnions(typechecker: Typechecker) extends OperatorOptimization with LazyLogging
{
  def decomposeAggregate(agg: AggFunction): Option[DecomposedAggregate] =
  {
    agg.function match {
      case "SUM" | "GROUP_AND" | "GROUP_OR" =>  
      {
        Some(DecomposedAggregate(
          ProjectArg(agg.alias, Var(agg.alias)),
          Seq(AggFunction(agg.function, agg.distinct, Seq(Var(agg.alias)), agg.alias)),
          Seq(agg)
        ))
      }
      case _ => 
      {
        logger.debug(s"Didn't decompose aggregate: ${agg.function}")
        None
      }
    }
  }

  def pullOutUnions(o: Operator): Seq[Operator] =
  {
    o match {
      case Union(a, b) => pullOutUnions(a) ++ pullOutUnions(b)
      case Select(cond, src) => pullOutUnions(src).map { Select(cond, _) }
      case Project(cols, src) => pullOutUnions(src).map { Project(cols, _) }
      case Join(lhs, rhs) => {
        val rhsUnions = pullOutUnions(rhs)
        pullOutUnions(lhs).flatMap { lhsElem => rhsUnions.map { Join(lhsElem, _) } }
      }
      case t: Table => Seq(t)
      case v: View => Seq(v)
      case HardTable(_,Seq()) => Seq()
      case _ => 
        Seq(o.recur(apply(_)))
    }
  }

  def apply(o: Operator): Operator =
  {
    val pulled = pullOutUnions(o)
    logger.trace(s"Apply: \n$o\n->\n$pulled")
    if(!pulled.isEmpty){
      OperatorUtils.makeUnion(pulled)
    } else {
      HardTable(typechecker.schemaOf(o),Seq())
    }
  }
}