package mimir.models;

import scala.util.Random

import mimir.algebra._
import mimir.util._
import mimir.ctables.VGTerm

/**
 * A model representing a key-repair choice.
 * 
 * The index is ignored.
 * The one argument is a value for the key.  
 * The return value is an integer identifying the ordinal position of the selected value, starting with 0.
 */
@SerialVersionUID(1000L)
class MissingKeyModel(override val name: String, keys:Seq[String], colTypes:Seq[Type]) 
  extends Model(name) 
  with Serializable
  with FiniteDiscreteDomain
{
  var acked = false

  def argTypes(idx: Int) = {
    if(idx < keys.length)
      Seq(TInt())
    else
      Seq(TAny()) 
  }
  def varType(idx: Int, args: Seq[Type]) = colTypes(idx)
  def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]  ) = {
    if(idx < keys.length)
      args(idx)
    else
      NullPrimitive() 
  }
  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]) = {
    if(idx < keys.length)
      args(idx)
    else
      NullPrimitive() 
  }
  def reason(idx: Int, args: Seq[PrimitiveValue],hints: Seq[PrimitiveValue]): String = {
    args(0) match {
      case NullPrimitive() => {
        "I guessed that the row of this cell was missing. The value of this cell is unknown so I have made it NULL."
      }
      case IntPrimitive(i) => {
        s"I guessed that this key was missing because it was in a sequence but not in the query results: $i" 
      }
      case FloatPrimitive(i)  => {
        s"I guessed that this key was missing because it was in a sequence but not in the query results: $i" 
      }
    }
  }
  def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit = { acked = true }
  def isAcknowledged (idx: Int, args: Seq[PrimitiveValue]): Boolean = acked
  def hintTypes(idx: Int): Seq[mimir.algebra.Type] = Seq()
  def getDomain(idx: Int, args: Seq[PrimitiveValue], hints:Seq[PrimitiveValue]): Seq[(PrimitiveValue,Double)] = Seq((args(0), 0.0))
  
     
}