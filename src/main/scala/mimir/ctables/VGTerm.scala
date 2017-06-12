package mimir.ctables

import mimir.algebra._
import mimir.models._
import scala.util._

case class VGTerm(
  model: Model, 
  idx: Int,
  args: Seq[Expression],
  hints: Seq[Expression]
) extends Proc(args++hints) {
  override def toString() = "{{ "+model.name+";"+idx+"["+args.mkString(", ")+"]["+hints.mkString(", ")+"] }}"
  override def getType(bindings: Seq[Type]):Type = model.varType(idx, bindings)
  override def children: Seq[Expression] = args ++ hints
  override def rebuild(x: Seq[Expression]) = {
    val (a, h) = x.splitAt(args.length)
    VGTerm(model, idx, a, h)
  }
  def get(v: Seq[PrimitiveValue]): PrimitiveValue = 
  {
    val (a, h) = v.splitAt(args.length)
    model.bestGuess(idx, a, h)
  }
  def isDataDependent: Boolean = args.size > 0
}