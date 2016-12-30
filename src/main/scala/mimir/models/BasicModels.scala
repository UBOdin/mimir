package mimir.models

import mimir.algebra._

import scala.util._

object UniformDistribution extends Model("UNIFORM") with Serializable {
  def argTypes(idx: Int) = List(TFloat(), TFloat())
  def varType(idx: Int, argTypes: Seq[Type]) = TFloat()
  def bestGuess(idx: Int, args: Seq[PrimitiveValue]) = 
    FloatPrimitive((args(0).asDouble + args(1).asDouble) / 2.0)
  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue]) = {
    val low = args(0).asDouble
    val high = args(1).asDouble
    FloatPrimitive(
      (randomness.nextDouble() * (high - low)) + high
    )
  }
  def bestGuessExpr(idx: Int, args: Seq[Expression]) = 
    Arithmetic(Arith.Div,
      Arithmetic(Arith.Add, args(0), args(1)),
      FloatPrimitive(2.0)
    )
  def sampleExpr(idx: Int, randomness: Expression, args: Seq[Expression]) = 
    Arithmetic(Arith.Add,
      Arithmetic(Arith.Mult,
        randomness,
        Arithmetic(Arith.Sub, args(1), args(0))
      ),
      args(0)
    )


  def reason(idx: Int, args: Seq[PrimitiveValue]): String = 
    "I put in a random value between "+args(0)+" and "+args(1)

  def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit =
    throw ModelException("Unsupported: Feedback on UniformDistribution")

  def isAcknowledged (idx: Int, args: Seq[PrimitiveValue]): Boolean =
    false
}

case class NoOpModel(override val name: String, reasonText:String) 
  extends Model(name) 
  with Serializable 
{
  var acked = false

  def argTypes(idx: Int) = List(TAny())
  def varType(idx: Int, args: Seq[Type]) = args(0)
  def bestGuess(idx: Int, args: Seq[PrimitiveValue]) = args(0)
  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue]) = args(0)
  def reason(idx: Int, args: Seq[PrimitiveValue]): String = reasonText
  def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit = { acked = true }
  def isAcknowledged (idx: Int, args: Seq[PrimitiveValue]): Boolean = acked
}
