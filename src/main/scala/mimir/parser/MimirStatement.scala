package mimir.parser

import sparsity._
import sparsity.statement._
import sparsity.expression._
import sparsity.select._
import play.api.libs.json._


sealed abstract class MimirStatement

case class SQLStatement(
  body: sparsity.statement.Statement
) extends MimirStatement;

case class Analyze(
  target: SelectBody,
  column: Option[Name],
  rowid: Option[StringPrimitive],
  withAssignments: Boolean
) extends MimirStatement

case class AnalyzeFeatures(
  target: SelectBody
) extends MimirStatement

case class Compare(
  target: SelectBody,
  expected: SelectBody
) extends MimirStatement

case class CreateLens(
  name: Name,
  body: SelectBody,
  lensType: Name,
  args: JsValue
) extends MimirStatement

case class PlotLine(
  x: Expression,
  y: Expression,
  args: Seq[(String, sparsity.expression.PrimitiveValue)]
)

case class DrawPlot(
  body: FromElement,
  lines: Seq[PlotLine],
  args: Seq[(String, sparsity.expression.PrimitiveValue)]
) extends MimirStatement

case class Feedback(
  model: Name,
  index: Long,
  args: Seq[PrimitiveValue],
  value: PrimitiveValue
) extends MimirStatement

case class Load(
  file: String,
  table: Option[Name],
  format: Option[Name],
  args: Seq[(String, sparsity.expression.PrimitiveValue)],
  withStaging: Boolean,
  linkOnly: Boolean
) extends MimirStatement
{
  def formatOrElse(alt: String) = format.getOrElse { sparsity.Name(alt) }
}

case class Reload(
  table: Name
) extends MimirStatement

case class DropLens(
  lens: Name,
  ifExists: Boolean = false
) extends MimirStatement

case class AlterTable(
  targetSchema: Option[Name],
  target: Name,
  op: AlterTableOperation
) extends MimirStatement

case class CreateSample(
  name: Name,
  mode: mimir.algebra.sampling.SamplingMode,
  source: Name,
  orReplace: Boolean,
  asView: Boolean = true,
  seed: Option[Long] = None
) extends MimirStatement