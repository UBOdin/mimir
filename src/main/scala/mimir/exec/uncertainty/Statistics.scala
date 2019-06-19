package mimir.exec.uncertainty

import mimir.algebra._

sealed abstract class Statistic

abstract class ColumnStatistic(
  input: ID, 
  aggName: ID, 
  output: ID, 
  distinct: Boolean = false
) 
  extends Statistic
{
  def aggregates: Seq[AggFunction] =
    Seq( AggFunction(aggName, distinct, Seq(Var(input)), output) )
  def projections: Seq[ProjectArg] = 
    Seq( ProjectArg(output, Var(output)) )
}

case class Expectation(inputColumn: ID, outputColumn: ID) 
  extends ColumnStatistic(inputColumn, ID("avg"), outputColumn)

case class StdDev(inputColumn: ID, outputColumn: ID) 
  extends ColumnStatistic(inputColumn, ID("stddev"), outputColumn)

case class AnyValue(inputColumn: ID, outputColumn: ID) 
  extends ColumnStatistic(inputColumn, ID("first"), outputColumn)

case class Confidence(output: ID) extends Statistic