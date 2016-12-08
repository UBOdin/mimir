package mimir.models

import mimir.algebra._

trait DataIndependentSingleVarFeedback {
  val name: String
  def validateChoice(v: PrimitiveValue): Boolean

  var choice: Option[PrimitiveValue] = None;

  def feedback(args: List[PrimitiveValue], v: PrimitiveValue): Unit =
    if(validateChoice(v)){ choice = Some(v) }
    else { throw ModelException(s"Invalid choice for $name: $v") }
  def isAcknowledged(args: List[PrimitiveValue]): Boolean =
    !choice.isEmpty
}