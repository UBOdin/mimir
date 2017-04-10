package mimir.ctables

import mimir.algebra._
import scala.util._
import mimir.models._
import java.sql.SQLException

case class VGTermSampler(
  model: Model, 
  idx: Int, 
  args: Seq[Expression], 
  hints: Seq[Expression], 
  seed: Expression
) 
  extends Proc(  (seed :: (args.toList ++ hints.toList))  )
{
  def getType(argTypes: Seq[Type]): Type =
    model.varType(idx, argTypes)
  def get(v: Seq[PrimitiveValue]): PrimitiveValue = 
  {
    if(v.size < 1){ throw new SQLException("Internal error.  Expecting seed.") }
    val seed = v.head
    val (argValues, hintValues) = v.tail.splitAt(args.length)
    model.sample(idx, seed.asLong, argValues, hintValues)
  }
  def rebuild(v: Seq[Expression]) = 
  {
    if(v.size < 1){ throw new SQLException("Internal error.  Expecting seed.") }
    var (a, h) = v.tail.splitAt(args.length)
    VGTermSampler(model, idx, a, h, v.head)
  }

}
