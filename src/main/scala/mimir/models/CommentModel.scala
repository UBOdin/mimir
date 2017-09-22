package mimir.models;

import scala.util.Random

import mimir.algebra._
import mimir.util._
import java.sql.SQLException

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
  with SourcedFeedback
{
  
  def getFeedbackKey(idx: Int, args: Seq[PrimitiveValue] ) : String = s"${args(0).asString}:$idx"
  
  def argTypes(idx: Int) = Seq(TRowId())
  def varType(idx: Int, args: Seq[Type]) = colTypes(idx)
  def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]  ) = {
    getFeedback(idx, args) match {
      case Some(v) => v
      case None => {
        hints(0)
      }
    }
  }
  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]) = hints(0)
  def reason(idx: Int, args: Seq[PrimitiveValue],hints: Seq[PrimitiveValue]): String = {
    //println("CommentModel:reason: " + idx + " [ " + args.mkString(",") + " ] [ " + hints.mkString(",") + " ]" );
    val rowid = RowIdPrimitive(args(0).asString)
    val rval = getFeedback(idx, args) match {
      case Some(v) => s"You told me that $v is valid for row $rowid"
      case None => s" ${comments(idx)}"
      case _ => throw new SQLException("This is impossible...")
    }
    rval
  }
  def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit = { 
    val rowid = args(0).asString
    setFeedback(idx, args, v)
  }
  def isAcknowledged (idx: Int, args: Seq[PrimitiveValue]): Boolean = hasFeedback(idx, args)
  def hintTypes(idx: Int): Seq[mimir.algebra.Type] = colTypes
  //def getDomain(idx: Int, args: Seq[PrimitiveValue], hints:Seq[PrimitiveValue]): Seq[(PrimitiveValue,Double)] = Seq((hints(0), 0.0))
  
     
}