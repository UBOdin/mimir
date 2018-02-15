package mimir.ctables.vgterm

import mimir.algebra._
import mimir.models._
import scala.util._
import mimir.ctables.RepairFromList
import mimir.ctables.RepairByType

case class DomainDumper(
  model: Model, 
  idx: Int,
  vgArgs: Seq[Expression],
  vgHints: Seq[Expression]
) extends Proc(vgArgs++vgHints) {
  override def toString() = "{{ DOMAIN DUMP: "+model.name+";"+idx+"["+vgArgs.mkString(", ")+"]["+vgHints.mkString(", ")+"] }}"
  override def getType(bindings: Seq[Type]):Type = model.varType(idx, bindings)
  override def children: Seq[Expression] = vgArgs ++ vgHints
  override def rebuild(x: Seq[Expression]) = {
    val (a, h) = x.splitAt(vgArgs.length)
    DomainDumper(model, idx, a, h)
  }
  def get(v: Seq[PrimitiveValue]): PrimitiveValue = 
  {
    val (a, h) = v.splitAt(vgArgs.length)
    val domain = model match {
      case finite:( Model with FiniteDiscreteDomain ) =>
        RepairFromList(finite.getDomain(idx, a, h))
      case _ => 
        RepairByType(model.varType(idx, a.map(_.getType)))
    }
    StringPrimitive(domain.toJSON)
  }
  def isDataDependent: Boolean = vgArgs.size > 0
}