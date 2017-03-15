package mimir.models;

import scala.util.Random

import mimir.algebra._
import mimir.util._

/**
 * A model representing a key-repair choice.
 * 
 * The index is ignored.
 * The one argument is a value for the key.  
 * The return value is an integer identifying the ordinal position of the selected value, starting with 0.
 */
@SerialVersionUID(1000L)
class CommentModel(override val name: String, cols:Seq[String], colTypes:Seq[Type], comments:Seq[String]) 
  extends Model(name) 
  with Serializable
  with FiniteDiscreteDomain
{
  var acked = false

  def argTypes(idx: Int) = {
    colTypes
  } 
  def varType(idx: Int, args: Seq[Type]) = {
    args(idx)
  }
  def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]  ) = {
    args(idx)
  }
  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]) = {
    args(idx)
  }
  def reason(idx: Int, args: Seq[PrimitiveValue],hints: Seq[PrimitiveValue]): String = {
    comments(idx)
  }
  def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit = { acked = true }
  def isAcknowledged (idx: Int, args: Seq[PrimitiveValue]): Boolean = acked
  def hintTypes(idx: Int): Seq[mimir.algebra.Type] = Seq(TAny())
  
  def getDomain(idx: Int, args: Seq[PrimitiveValue]): Seq[(PrimitiveValue,Double)] = {
    Seq((args(idx), 0.0))
  } 
     
}