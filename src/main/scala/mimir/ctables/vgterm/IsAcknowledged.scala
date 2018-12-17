package mimir.ctables.vgterm

import mimir.algebra._
import scala.util._
import mimir.models._
import java.sql.SQLException

case class IsAcknowledged(
  model: Model, 
  idx: Int, 
  vgArgs: Seq[Expression]
) 
  extends Proc( vgArgs )
{
  def getType(argTypes: Seq[BaseType]): BaseType = TBool()
  def get(v: Seq[PrimitiveValue]): PrimitiveValue = 
  {
    BoolPrimitive(model.isAcknowledged(idx, v))
  }
  def rebuild(v: Seq[Expression]) = 
    IsAcknowledged(model, idx, v)

  override def toString =
    s"VGTERM_ACKNOWLEDGED(${(Seq(model.name, idx.toString) ++ vgArgs.map(_.toString)).mkString(", ")})"
}
