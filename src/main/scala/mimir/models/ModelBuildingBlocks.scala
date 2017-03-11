package mimir.models

import mimir.algebra._

trait NoArgModel
{
  def argTypes(x: Int): Seq[Type] = List()
}
trait NoArgSingleVarModel
{
  def argTypes(): Seq[Type] = List()
}