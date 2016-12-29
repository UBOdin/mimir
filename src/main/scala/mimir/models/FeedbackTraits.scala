package mimir.models

import mimir.algebra._

trait DataIndependentSingleVarFeedback {
  val name: String
  def validateChoice(v: PrimitiveValue): Boolean

  var choice: Option[PrimitiveValue] = None;

  def feedback(args: Seq[PrimitiveValue], v: PrimitiveValue): Unit =
    if(validateChoice(v)){ choice = Some(v) }
    else { throw ModelException(s"Invalid choice for $name: $v") }
  def isAcknowledged(args: Seq[PrimitiveValue]): Boolean =
    !choice.isEmpty
}

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