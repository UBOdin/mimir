package mimir.models;

import scala.util.Random

import mimir.algebra._
import mimir.util._

/**
 * A dumb, default Meta-Model to stand in until we get something better.
 *
 * This meta model always ignores VG arguments and picks the first model
 * in the list.
 */
@SerialVersionUID(1000L)
class DefaultMetaModel(name: String, context: String, models: Seq[String]) 
  extends Model(name) 
  with DataIndependentFeedback 
  with NoArgModel
  with FiniteDiscreteDomain
{
  def varType(idx: Int, args: Seq[Type]): Type = TString()
  def bestGuess(idx: Int, args: Seq[PrimitiveValue]): PrimitiveValue =
    choices.getOrElse(idx, StringPrimitive(models.head))
  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue]): PrimitiveValue =
    StringPrimitive(RandUtils.pickFromList(randomness, models))
  def reason(idx: Int, args: Seq[PrimitiveValue]): String =
  {
    choices.get(idx) match {
      case None => {
        val bestChoice = models.head
        val modelString = models.mkString(", ")
        s"I defaulted to guessing with '$bestChoice' (out of $modelString) for $context"
      }
      case Some(choiceStr) => 
        s"You told me to use the $choiceStr model for $context"
    }
  }
  def validateChoice(idx: Int, v: PrimitiveValue) = models.contains(v.asString)

  def getDomain(idx: Int, args: Seq[PrimitiveValue]): Seq[(PrimitiveValue,Double)] =
    models.map( x => (StringPrimitive(x), 0.0) )

}