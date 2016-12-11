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
  extends SingleVarModel(name) 
  with DataIndependentSingleVarFeedback 
  with Serializable
{
  def varType(argTypes:Seq[Type]) = TString()

  def bestGuess(args: Seq[PrimitiveValue]): PrimitiveValue =
    choice.getOrElse(StringPrimitive(models.head))
  def sample(randomness: Random, args: Seq[PrimitiveValue]): PrimitiveValue =
    StringPrimitive(RandUtils.pickFromList(randomness, models))
  def reason(args: Seq[PrimitiveValue]): String =
  {
    choice match {
      case None => {
        val bestChoice = models.head
        val modelString = models.mkString(", ")
        s"I defaulted to guessing with '$bestChoice' (out of $modelString) for $context"
      }
      case Some(choiceStr) => 
        s"You told me to use the $choiceStr model for $context"
    }
  }
  def validateChoice(v: PrimitiveValue) = models.contains(v.asString)
}