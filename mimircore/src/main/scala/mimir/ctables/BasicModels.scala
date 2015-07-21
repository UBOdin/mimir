package mimir.ctables

import mimir.algebra._

import scala.util._

abstract class SingleVarModel(vt: Type.T) extends Model {
  def varType = vt
  def varTypes = List(vt)

  def mostLikelyValue(args: List[PrimitiveValue]): PrimitiveValue
  def lowerBound(args: List[PrimitiveValue]): PrimitiveValue
  def upperBound(args: List[PrimitiveValue]): PrimitiveValue
  def sampleGenerator(args: List[PrimitiveValue]): PrimitiveValue
  def mostLikelyExpr(args: List[Expression]): Expression
  def lowerBoundExpr(args: List[Expression]): Expression
  def upperBoundExpr(args: List[Expression]): Expression
  def sampleGenExpr(args: List[Expression]): Expression
  def sample(seed: Long, args: List[PrimitiveValue]): PrimitiveValue
  def reason(): String

  def mostLikelyValue(x:Int, args: List[PrimitiveValue]) = mostLikelyValue(args)
  def lowerBound(x: Int, args: List[PrimitiveValue]) = lowerBound(args)
  def upperBound(x: Int, args: List[PrimitiveValue]) = upperBound(args)
  def sampleGenerator(x: Int, args: List[PrimitiveValue]) = sampleGenerator(args)
  def mostLikelyExpr(idx: Int, args: List[Expression]) = mostLikelyExpr(args)
  def lowerBoundExpr(x: Int, args: List[Expression]) = lowerBoundExpr(args)
  def upperBoundExpr(x: Int, args: List[Expression]) = upperBoundExpr(args)
  def sampleGenExpr(x: Int, args: List[Expression]) = sampleGenExpr(args)
  def sample(seed: Long, x: Int, args: List[PrimitiveValue]) = sample(seed, args)
  def reason(x: Int): String = reason()

}

case class JointSingleVarModel(vars: List[SingleVarModel]) extends Model {
  def varTypes = vars.map( _.varType )

  def mostLikelyValue  (idx: Int, args: List[PrimitiveValue]) = 
    vars(idx).mostLikelyValue(args);
  def lowerBound     (idx: Int, args: List[PrimitiveValue]) = 
    vars(idx).lowerBound(args);
  def upperBound     (idx: Int, args: List[PrimitiveValue]) = 
    vars(idx).upperBound(args);
  def sampleGenerator (idx: Int, args: List[PrimitiveValue]) =
    vars(idx).sampleGenerator(args)
  def mostLikelyExpr (idx: Int, args: List[Expression]) =
    vars(idx).mostLikelyExpr(args)
  def lowerBoundExpr (idx: Int, args: List[Expression]) = 
    vars(idx).lowerBoundExpr(args)
  def upperBoundExpr (idx: Int, args: List[Expression]) = 
    vars(idx).upperBoundExpr(args)
  def sampleGenExpr (idx: Int, args: List[Expression]) =
    vars(idx).sampleGenExpr(args)
  def sample(seed: Long, idx: Int, args: List[PrimitiveValue]) = 
    vars(idx).sample(seed, args)

  override def reason(idx: Int): String =
    vars(idx).reason
}

object UniformDistribution extends SingleVarModel(Type.TFloat){
  def mostLikelyValue(args: List[PrimitiveValue]) = 
    FloatPrimitive((args(0).asDouble + args(1).asDouble) / 2.0)
  def lowerBound(args: List[PrimitiveValue]) = args(0)
  def upperBound(args: List[PrimitiveValue]) = args(1)
  def sampleGenerator(args: List[PrimitiveValue]) = sample(args.last.asLong, args)
  def mostLikelyExpr(args: List[Expression]) = args(0)
  def lowerBoundExpr(args: List[Expression]) = args(0)
  def upperBoundExpr(args: List[Expression]) = args(1)
  def sampleGenExpr(args: List[Expression]) = args(0)
  def sample(seed: Long, args: List[PrimitiveValue]) = {
    val low  = args(0).asDouble
    val high = args(1).asDouble
    FloatPrimitive(new Random(seed).nextDouble() * (high - low) + low)
  }

  override def reason(): String = "Unknown"   // TODO
}

case class NoOpModel(vt: Type.T) extends SingleVarModel(vt) {
  def mostLikelyValue(args: List[PrimitiveValue]) = args(0)
  def lowerBound(args: List[PrimitiveValue]) = args(0)
  def upperBound(args: List[PrimitiveValue]) = args(0)
  def sampleGenerator(args: List[PrimitiveValue]) = sample(args.last.asLong, args)
  def mostLikelyExpr(args: List[Expression]) = args(0)
  def lowerBoundExpr(args: List[Expression]) = args(0)
  def upperBoundExpr(args: List[Expression]) = args(0)
  def sampleGenExpr(args: List[Expression]) = args(0)
  def sample(seed: Long, args: List[PrimitiveValue]) = args(0)

  override def reason(): String = "None"
}