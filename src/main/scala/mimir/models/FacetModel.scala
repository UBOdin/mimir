package mimir.models

import mimir.algebra._
import mimir.statistics.facet.Facet


@SerialVersionUID(100L)
class FacetModel(name: String, facet: Facet) 
  extends Model(name)
  with NoArgModel
{
  var isAcked = false;

  def bestGuess(idx: Int,args: Seq[PrimitiveValue],hints: Seq[PrimitiveValue]): PrimitiveValue = 
    BoolPrimitive(true)
  def confidence(idx: Int,args: Seq[PrimitiveValue],hints: Seq[PrimitiveValue]): Double = 
    0.0
  def feedback(idx: Int,args: Seq[PrimitiveValue],v: PrimitiveValue): Unit = 
  {
    isAcked = v match {
      case BoolPrimitive(v) => v
      case _ => throw new RAException(s"Invalid acknowledgement '${v}' for '${facet.description}'")
    }
  }
  def isAcknowledged(idx: Int,args: Seq[PrimitiveValue]): Boolean = 
    isAcked
  def reason(idx: Int,args: Seq[PrimitiveValue],hints: Seq[PrimitiveValue]): String = 
    facet.description
  def sample(idx: Int,randomness: scala.util.Random,args: Seq[PrimitiveValue],hints: Seq[PrimitiveValue]): PrimitiveValue = 
    BoolPrimitive(true)
  def varType(idx: Int,argTypes: Seq[Type]): Type = 
    TBool()
}