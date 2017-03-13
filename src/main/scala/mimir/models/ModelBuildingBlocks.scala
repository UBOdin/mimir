package mimir.models

import mimir.algebra._

trait NoArgModel
{
  def argTypes(x: Int): Seq[Type] = List()
  def hintTypes(idx: Int) = Seq()
}