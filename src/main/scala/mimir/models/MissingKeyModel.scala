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
@SerialVersionUID(1001L)
class MissingKeyModel(override val name: String, keys:Seq[String], colTypes:Seq[Type]) 
  extends Model(name) 
  with Serializable
  with FiniteDiscreteDomain
  with SourcedFeedback
{
  
  def getFeedbackKey(idx: Int, args: Seq[PrimitiveValue] ) : String = s"${args(0).asString}_$idx"
  
  def argTypes(idx: Int) = {
      Seq(TRowId())
  }
  def varType(idx: Int, args: Seq[Type]) = colTypes(idx)
  def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]  ) = {
    //println(s"MissingKeyModel:bestGuess: idx: $idx args: ${args.mkString("[ ",","," ]")} hints: ${hints.mkString("[ ",","," ]")}")
    getFeedback(idx, args) match {
      case Some(v) => v
      case None => hints(0) 
    }
  }
  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]) = {
    hints(0)
  }
  def reason(idx: Int, args: Seq[PrimitiveValue],hints: Seq[PrimitiveValue]): String = {
    val rowid = RowIdPrimitive(args(0).asString)
    getFeedback(idx, args) match {
      case Some(v) => v match {
          case NullPrimitive() => {
            "You told me that the row of this cell was missing and that the value of this cell is unknown so I have made it NULL."
          }
          case i => {
            s"You told me that this key was missing because it was in a sequence but not in the query results: $i" 
          }
      }
      case None => hints(0) match {
        case NullPrimitive() => {
          "I guessed that the row of this cell was missing. The value of this cell is unknown so I have made it NULL."
        }
        case i => {
          s"I guessed that this key was missing because it was in a sequence but not in the query results: $i" 
        }
      }
    }
  }
  def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit = { 
    setFeedback(idx, args, v)
  }
  def isAcknowledged (idx: Int, args: Seq[PrimitiveValue]): Boolean = {
    hasFeedback(idx, args)
  }
  def hintTypes(idx: Int): Seq[mimir.algebra.Type] = Seq(TAny())
  def getDomain(idx: Int, args: Seq[PrimitiveValue], hints:Seq[PrimitiveValue]): Seq[(PrimitiveValue,Double)] = Seq((hints(0), 0.0))
  
     
}