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
  val feedback = scala.collection.mutable.Map[String,PrimitiveValue]()
  
  def argTypes(idx: Int) = {
      Seq(TRowId())
  }
  def varType(idx: Int, args: Seq[Type]) = colTypes(idx)
  def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]  ) = {
    val rowid = RowIdPrimitive(args(0).asString)
    feedback.get(rowid.asString) match {
      case Some(v) => v
      case None => hints(0) 
    }
  }
  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]) = {
    hints(idx)
  }
  def reason(idx: Int, args: Seq[PrimitiveValue],hints: Seq[PrimitiveValue]): String = {
    val rowid = RowIdPrimitive(args(0).asString)
    feedback.get(rowid.asString) match {
      case Some(v) => v match {
          case NullPrimitive() => {
            "You told me that the row of this cell was missing and that he value of this cell is unknown so I have made it NULL."
          }
          case IntPrimitive(i) => {
            s"You told me that this key was missing because it was in a sequence but not in the query results: $i" 
          }
          case FloatPrimitive(i)  => {
            s"You told me that this key was missing because it was in a sequence but not in the query results: $i" 
          }
      }
      case None => hints(0) match {
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
  }
  def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit = { 
    val rowid = args(0).asString
    feedback(rowid) = v
  }
  def isAcknowledged (idx: Int, args: Seq[PrimitiveValue]): Boolean = {
    feedback contains(args(0).asString)
  }
  def hintTypes(idx: Int): Seq[mimir.algebra.Type] = Seq(TAny())
  def getDomain(idx: Int, args: Seq[PrimitiveValue], hints:Seq[PrimitiveValue]): Seq[(PrimitiveValue,Double)] = Seq((hints(0), 0.0))
  
     
}