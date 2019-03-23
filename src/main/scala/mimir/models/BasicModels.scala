package mimir.models

import mimir.algebra._

import scala.util._

object UniformDistribution extends Model(ID("UNIFORM")) with Serializable {
  def argTypes(idx: Int) = List(TFloat(), TFloat())
  def hintTypes(idx: Int) = Seq()
  def varType(idx: Int, argTypes: Seq[Type]) = TFloat()
  def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]) = 
    FloatPrimitive((args(0).asDouble + args(1).asDouble) / 2.0)
  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]) = {
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


  def reason(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): String = 
    "I put in a random value between "+args(0)+" and "+args(1)

  def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit =
    throw ModelException("Unsupported: Feedback on UniformDistribution")

  def isAcknowledged (idx: Int, args: Seq[PrimitiveValue]): Boolean =
    false

  // Random, so...
  def confidence (idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): Double = 0.5
}

case class NoOpModel(override val name: ID, reasonText:String) 
  extends Model(name) 
  with Serializable 
{
  var acked = false

  def argTypes(idx: Int) = List(TAny())
  def hintTypes(idx: Int) = Seq()
  def varType(idx: Int, args: Seq[Type]) = args(0)
  def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]) = args(0)
  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]) = args(0)
  def reason(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): String = reasonText
  def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit = { acked = true }
  def isAcknowledged (idx: Int, args: Seq[PrimitiveValue]): Boolean = acked
  def confidence(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): Double = 0.5 // Random, so...
}

case class WarningModel(override val name: ID, keyTypes: Seq[Type])
  extends Model(name)
  with Serializable
  with SourcedFeedback
{
  def argTypes(idx: Int) = keyTypes
  def hintTypes(idx: Int) = Seq(TAny(), TString())
  def varType(idx: Int, args: Seq[Type]) = TString()
  def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]) = 
    StringPrimitive("I have no idea?")
  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]) = 
    args(0)
  def reason(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): String = 
    hints(1).asString
  def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit = setFeedback(0, args, v)
  def isAcknowledged (idx: Int, args: Seq[PrimitiveValue]): Boolean = hasFeedback(0, args)
  def confidence(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): Double = 0.0
  def getFeedbackKey(idx: Int, args: Seq[PrimitiveValue]) = ID(args.map { _.toString }.mkString(":::"))
}