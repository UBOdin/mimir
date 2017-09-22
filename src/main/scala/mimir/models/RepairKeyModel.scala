package mimir.models;

import scala.util.Random

import play.api.libs.json._

import mimir.algebra._
import mimir.util._
import mimir.serialization.Json
import mimir.Database

/**
 * A model representing a key-repair choice.
 * 
 * The index is ignored.
 * The one argument is a value for the key.  
 * The return value is an integer identifying the ordinal position of the selected value, starting with 0.
 */
@SerialVersionUID(1002L)
class RepairKeyModel(
  name: String, 
  context: String, 
  source: Operator, 
  keys: Seq[(String, Type)], 
  target: String,
  targetType: Type,
  scoreCol: Option[String]
) 
  extends Model(name)
  with FiniteDiscreteDomain 
  with NeedsReconnectToDatabase 
  with SourcedFeedbackT[List[PrimitiveValue]]
{
  
  @transient var db:Database = null

  def getFeedbackKey(idx: Int, args: Seq[PrimitiveValue]) : List[PrimitiveValue] = args.toList
  
  def varType(idx: Int, args: Seq[Type]): Type = targetType
  def argTypes(idx: Int) = keys.map(_._2)
  def hintTypes(idx: Int) = Seq(TString(), TString())

  def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): PrimitiveValue =
    getFeedback(idx, args) match {
      case Some(choice) => choice
      case None => getDomain(idx, args, hints).sortBy(-_._2).head._1
    }

  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): PrimitiveValue = 
    RandUtils.pickFromWeightedList(randomness, getDomain(idx, args, hints))

  def reason(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): String =
  {
    getFeedback(idx, args) match {
      case None => {
        val possibilities = getDomain(idx, args, hints)
        s"In $context, there were ${possibilities.length} options for $target on the row identified by <${args.map(_.toString).mkString(", ")}>, and I arbitrarilly picked ${possibilities.sortBy(-_._2).head._1}"
      }
      case Some(choice) => 
        s"In $context, you told me to use ${choice.toString} for $target on the identified by <${args.map(_.toString).mkString(", ")}>"
    }
  }

  def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit =
    setFeedback(idx, args, v)
  def isAcknowledged(idx: Int, args: Seq[PrimitiveValue]): Boolean =
    hasFeedback(idx, args)


  final def getDomain(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): Seq[(PrimitiveValue,Double)] =
  {
    if(hints.isEmpty){
      db.query(
        OperatorUtils.projectColumns(List(target) ++ scoreCol, 
          Select(
            ExpressionUtils.makeAnd(
              keys.map(_._1).zip(args).map { 
                case (k,v) => Comparison(Cmp.Eq, Var(k), v)
              }
            ),
            source
          )
        )
      ){ _.map { row => 
          ( row(0), 
            scoreCol match { 
              case None => 1.0; 
              case Some(_) => row(1).asDouble
            }
          )
        }.toSeq
      }
    } else {
      val possibilities = 
        Json.parse(hints(0).asString) match {
          case JsArray(values) => values.map { Json.toPrimitive(targetType, _)  }
          case _ => throw ModelException(s"Invalid Value Hint in Repair Model $name: ${hints(0).asString}")
        }

      val possibilitiesWithProbabilities =
        if(hints.size > 1 && !hints(1).isInstanceOf[NullPrimitive]){
          possibilities.zip(
            Json.parse(hints(1).asString) match {
              case JsArray(values) => values.map( v => Json.toPrimitive(TFloat(), v).asDouble )
              case _ => throw ModelException(s"Invalid Score Hint in Repair Model $name: ${hints(1).asString}")
            }
          )
        } else {
          possibilities.map( (_, 1.0) )
        }

      val goodPossibilities =
        possibilitiesWithProbabilities.filter(!_._1.isInstanceOf[NullPrimitive])

      if(goodPossibilities.isEmpty){
        Seq((NullPrimitive(), 1.0))
      } else {
        goodPossibilities
      }

    }
  }
  
  def reconnectToDatabase(db: Database) = { 
    this.db = db 
  }

}
