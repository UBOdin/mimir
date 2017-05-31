package mimir.optimizer

import mimir.algebra._

trait OperatorOptimization 
{
  def apply(o: Operator): Operator
}