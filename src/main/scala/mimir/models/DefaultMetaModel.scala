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
class DefaultMetaModel(name: String, context: String, models: List[String]) 
  extends SingleVarModel(name) 
  with DataIndependentSingleVarFeedback 
  with FiniteDiscreteDomain
{
  def varType(argTypes:List[Type]) = TString()

  def bestGuess(args: List[PrimitiveValue]): PrimitiveValue =
    choice.getOrElse(StringPrimitive(models.head))
  def sample(randomness: Random, args: List[PrimitiveValue]): PrimitiveValue =
    StringPrimitive(RandUtils.pickFromList(randomness, models))
  def reason(args: List[PrimitiveValue]): String =
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

  def getDomain(idx: Int, args: List[PrimitiveValue]): Seq[(PrimitiveValue,Double)] =
    models.map( x => (StringPrimitive(x), 0.0) )

}