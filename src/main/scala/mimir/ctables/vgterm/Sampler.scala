package mimir.ctables.vgterm

import mimir.algebra._
import scala.util._
import mimir.models._
import java.sql.SQLException

case class Sampler(
  model: Model, 
  idx: Int, 
  vgArgs: Seq[Expression], 
  vgHints: Seq[Expression], 
  seed: Expression
) 
  extends Proc(  (seed :: (vgArgs.toList ++ vgHints.toList))  )
{
  def getType(argTypes: Seq[BaseType]): BaseType =
    model.varType(idx, argTypes)
  def get(v: Seq[PrimitiveValue]): PrimitiveValue = 
  {
    if(v.size < 1){ throw new SQLException("Internal error.  Expecting seed.") }
    val seed = v.head
    val (argValues, hintValues) = v.tail.splitAt(vgArgs.length)
    model.sample(idx, seed.asLong, argValues, hintValues)
  }
  def rebuild(v: Seq[Expression]) = 
  {
    if(v.size < 1){ throw new SQLException("Internal error.  Expecting seed.") }
    var (a, h) = v.tail.splitAt(vgArgs.length)
    Sampler(model, idx, a, h, v.head)
  }

}
