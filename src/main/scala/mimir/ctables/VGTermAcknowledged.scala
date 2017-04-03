package mimir.ctables

import mimir.algebra._
import scala.util._
import mimir.models._
import java.sql.SQLException

case class VGTermAcknowledged(
  model: Model, 
  idx: Int, 
  args: Seq[Expression]
) 
  extends Proc( args )
{
  def getType(argTypes: Seq[Type]): Type = TBool()
  def get(v: Seq[PrimitiveValue]): PrimitiveValue = 
  {
    BoolPrimitive(model.isAcknowledged(idx, v))
  }
  def rebuild(v: Seq[Expression]) = 
    VGTermAcknowledged(model, idx, v)

  override def toString =
    s"VGTERM_ACKNOWLEDGED(${(Seq(model.name, idx.toString) ++ args.map(_.toString)).mkString(", ")})"
}
