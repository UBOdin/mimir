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
class CommentModel(override val name: String, col:String, comment:String) 
  extends Model(name) 
  with Serializable
{
  var acked = false

  def argTypes(idx: Int) = List(TAny())
  def varType(idx: Int, args: Seq[Type]) = args(0)
  def bestGuess(idx: Int, args: Seq[PrimitiveValue]) = args(0)
  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue]) = args(0)
  def reason(idx: Int, args: Seq[PrimitiveValue]): String = comment
  def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit = { acked = true }
  def isAcknowledged (idx: Int, args: Seq[PrimitiveValue]): Boolean = acked
}