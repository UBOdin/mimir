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
class DefaultMetaModel(name: String, context: String, models: List[String]) 
  extends SingleVarModel(name) with Serializable
{

  def varType(argTypes:List[Type.T]) = Type.TString

  def bestGuess(args: List[PrimitiveValue]): PrimitiveValue =
    StringPrimitive(models.head)
  def sample(randomness: Random, args: List[PrimitiveValue]): PrimitiveValue =
    StringPrimitive(RandUtils.pickFromList(randomness, models))
  def reason(args: List[Expression]): String =
  {
    val bestChoice = models.head
    val modelString = models.mkString(", ")
    s"I defaulted to guessing with '$bestChoice' (out of $modelString) for $context"
  }
}