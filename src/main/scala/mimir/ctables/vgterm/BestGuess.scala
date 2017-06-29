package mimir.ctables.vgterm

import mimir.algebra._
import mimir.models._
import scala.util._

case class BestGuess(
  model: Model, 
  idx: Int,
  vgArgs: Seq[Expression],
  vgHints: Seq[Expression]
) extends Proc(vgArgs++vgHints) {
  override def toString() = "{{ "+model.name+";"+idx+"["+vgArgs.mkString(", ")+"]["+vgHints.mkString(", ")+"] }}"
  override def getType(bindings: Seq[Type]):Type = model.varType(idx, bindings)
  override def children: Seq[Expression] = vgArgs ++ vgHints
  override def rebuild(x: Seq[Expression]) = {
    val (a, h) = x.splitAt(vgArgs.length)
    BestGuess(model, idx, a, h)
  }
  def get(v: Seq[PrimitiveValue]): PrimitiveValue = 
  {
    val (a, h) = v.splitAt(vgArgs.length)
    model.bestGuess(idx, a, h)
  }
  def isDataDependent: Boolean = vgArgs.size > 0
}