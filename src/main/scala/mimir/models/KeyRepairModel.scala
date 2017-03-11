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
class KeyRepairModel(
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
  with NeedsDatabase 
{
  val choices = scala.collection.mutable.Map[List[PrimitiveValue], PrimitiveValue]();

  def varType(idx: Int, args: Seq[Type]): Type = targetType
  def argTypes(idx: Int) = keys.map(_._2)
  def hintTypes(idx: Int) = Seq()

  def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): PrimitiveValue =
    choices.get(args.toList) match {
      case Some(choice) => choice
      case None => getDomain(idx, args).sortBy(-_._2).head._1
    }

  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): PrimitiveValue = 
    RandUtils.pickFromWeightedList(randomness, getDomain(idx, args))

  def reason(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): String =
  {
    choices.get(args.toList) match {
      case None => {
        val possibilities = getDomain(idx, args)
        s"In $context, there were ${possibilities.length} options for $target on the row for <${args.map(_.toString).mkString(", ")}>, and I arbitrarilly picked ${possibilities.sortBy(_.toString).head}"
      }
      case Some(choice) => 
        s"In $context, you told me to use ${choice.toString} for $target on the row for <${args.map(_.toString).mkString(", ")}>"
    }
  }

  def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit =
    choices(args.toList) = v
  def isAcknowledged(idx: Int, args: Seq[PrimitiveValue]): Boolean =
    choices contains args.toList


  final def getDomain(idx: Int, args: Seq[PrimitiveValue]): Seq[(PrimitiveValue,Double)] =
  {
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
    ).mapRows { row => 
      ( row(0), 
        scoreCol match { 
          case None => 1.0; 
          case Some(_) => row(1).asDouble
        }
      )
    }.toSeq
  }

}