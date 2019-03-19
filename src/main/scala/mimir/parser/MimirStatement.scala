package mimir.parser

import java.io.File
import sparsity._
import sparsity.statement._
import sparsity.expression._
import sparsity.select._

sealed abstract class MimirStatement

case class SQLStatement(
  body: sparsity.statement.Statement
) extends MimirStatement;

case class SlashCommand(
  body: String
) extends MimirStatement

case class Analyze(
  target: Select,
  column: Option[Name],
  rowid: Option[StringPrimitive],
  withAssignments: Boolean
) extends MimirStatement

case class AnalyzeFeatures(
  target: Select
) extends MimirStatement

case class Compare(
  target: Select,
  expected: Select
) extends MimirStatement

case class CreateAdaptiveSchema(
  name: Name,
  body: SelectBody,
  schemaType: Name,
  args: Seq[Expression]
) extends MimirStatement

case class CreateLens(
  name: Name,
  body: SelectBody,
  lensType: Name,
  args: Seq[Expression]
) extends MimirStatement

case class PlotLine(
  x: Expression,
  y: Expression,
  args: Seq[(String -> sparsity.expression.PrimitiveValue)]
) extends MimirStatement

case class DrawPlot(
  body: FromElement,
  lines: Seq[PlotLine],
  args: Seq[(String -> sparsity.expression.PrimitiveValue)]
) extends MimirStatement

case class Feedback(
  model: Name,
  index: Int,
  args: Seq[PrimitiveValue],
  value: PrimitiveValue
) extends MimirStatement

case class Load(
  file: File,
  table: Option[Name],
  format: Option[Name],
  args: Seq[sparsity.expression.Expression]
) extends MimirStatement

case class DropLens(
  lens: Name,
  ifExists: Boolean = false
) extends MimirStatement

case class DropAdaptiveSchema(
  schema: Name,
  ifExists: Boolean = false
) extends MimirStatement