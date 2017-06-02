package mimir.exec.uncertainty

import mimir.algebra._

sealed abstract class Statistic

abstract class ColumnStatistic(
  input: String, 
  aggName: String, 
  output: String, 
  distinct: Boolean = false
) 
  extends Statistic
{
  def aggregates: Seq[AggFunction] =
    Seq( AggFunction(aggName, distinct, Seq(Var(input)), output) )
  def projections: Seq[ProjectArg] = 
    Seq( ProjectArg(output, Var(output)) )
}

case class Expectation(inputColumn: String, outputColumn: String) 
  extends ColumnStatistic(inputColumn, "AVG", outputColumn)

case class StdDev(inputColumn: String, outputColumn: String) 
  extends ColumnStatistic(inputColumn, "STDDEV", outputColumn)

case class AnyValue(inputColumn: String, outputColumn: String) 
  extends ColumnStatistic(inputColumn, "FIRST", outputColumn)

case class Confidence(output: String) extends Statistic