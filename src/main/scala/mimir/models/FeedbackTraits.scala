package mimir.models

import mimir.algebra._

trait DataIndependentFeedback {
  val name: String
  def validateChoice(idx: Int, v: PrimitiveValue): Boolean

  var choices = scala.collection.mutable.Map[Int, PrimitiveValue]();

  def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit =
    if(validateChoice(idx, v)){ choices(idx) = v }
    else { throw ModelException(s"Invalid choice for $name: $v") }
  def isAcknowledged(idx: Int, args: Seq[PrimitiveValue]): Boolean =
    choices contains idx
}