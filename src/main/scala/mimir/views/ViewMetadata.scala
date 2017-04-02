package mimir.views

import mimir.algebra._

class ViewMetadata(
  val name: String,
  val query: Operator,
  val isMaterialized: Boolean
)
{
  def operator: Operator =
    View(name, query, Set())

  def schema =
    query.schema
}

object ViewMetadata
  extends Enumeration
{
  type T = Value
  val BEST_GUESS, TAINT, PROVENANCE = Value
}

