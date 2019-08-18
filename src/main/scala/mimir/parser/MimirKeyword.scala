package mimir.parser

import fastparse._, NoWhitespace._
import sparsity.parser._

object MimirKeyword
{
  def any[_:P] = P(
    Elements.anyKeyword | (
      StringInIgnoreCase(
        "ADAPTIVE",
        "ANALYZE",
        "ASSIGNMENTS",
        "COMPARE",
        "DRAW",
        "FEATURES",
        "FEEDBACK",
        "INTO",
        "LENS",
        "LINK",
        "LOAD",
        "OF",
        "PLOT",
        "PROVENANCE",
        "DEPENDENCY",
        "RELOAD",
        "SCHEMA",
        "STAGING",
        "USING"
      ).! ~ !CharIn("a-zA-Z0-9_") 
    )
  )

	def apply[_:P](expected: String*) = P(
    any.filter { kw => expected.exists { _.equalsIgnoreCase(kw) } }
       .map { _ => () }
  )
}