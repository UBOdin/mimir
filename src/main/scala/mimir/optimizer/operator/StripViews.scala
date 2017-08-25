package mimir.optimizer.operator

import java.sql._

import mimir.algebra._
import mimir.ctables._

object StripViews 
{

  def apply(o: Operator, onlyProbabilistic: Boolean = false): Operator =
  {
    if(onlyProbabilistic && CTables.isDeterministic(o)){ return o }
    o match { 
      case View(_, query, _) => apply(query)
      case _ => o.recur(apply(_))
    }
  }
}