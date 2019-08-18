package mimir.parser

import sparsity._

sealed trait AlterTableOperation

case class CreateDependency(
  sourceSchema: Option[Name],
  source: Name
) extends AlterTableOperation

case class DropDependency(
  sourceSchema: Option[Name],
  source: Name
) extends AlterTableOperation