package mimir.ctables

import scala.util._

import mimir.Database
import mimir.algebra._

abstract class SingleVarModel(vt: Type.T) extends Model {
  def varType = vt
  def varTypes = List(vt)

  def mostLikelyValue(args: List[PrimitiveValue]): PrimitiveValue;
  def lowerBound(args: List[PrimitiveValue]): PrimitiveValue
  def upperBound(args: List[PrimitiveValue]): PrimitiveValue
  def lowerBoundExpr(args: List[Expression]): Expression
  def upperBoundExpr(args: List[Expression]): Expression
  def sample(seed: Long, args: List[PrimitiveValue]):  PrimitiveValue

  def mostLikelyValue(x:Int, args: List[PrimitiveValue]) = mostLikelyValue(args);
  def lowerBound(x: Int, args: List[PrimitiveValue]) = lowerBound(args)
  def upperBound(x: Int, args: List[PrimitiveValue]) = upperBound(args)
  def lowerBoundExpr(x: Int, args: List[Expression]) = lowerBoundExpr(args)
  def upperBoundExpr(x: Int, args: List[Expression]) = upperBoundExpr(args)
  def sample(seed: Long, x: Int, args: List[PrimitiveValue]) = sample(seed,args);
}

case class JointSingleVarModel(vars: List[SingleVarModel]) extends Model {
  def varTypes = vars.map( _.varType )

  def mostLikelyValue  (idx: Int, args: List[PrimitiveValue]) = 
    vars(idx).mostLikelyValue(args);
  def lowerBound     (idx: Int, args: List[PrimitiveValue]) = 
    vars(idx).lowerBound(args);
  def upperBound     (idx: Int, args: List[PrimitiveValue]) = 
    vars(idx).upperBound(args);
  def lowerBoundExpr (idx: Int, args: List[Expression]) = 
    vars(idx).lowerBoundExpr(args);
  def upperBoundExpr (idx: Int, args: List[Expression]) = 
    vars(idx).upperBoundExpr(args);
  def sample(seed: Long, idx: Int, args: List[PrimitiveValue]) = 
    vars(idx).sample(seed, args);
}

object UniformDistribution extends SingleVarModel(Type.TFloat){
  def mostLikelyValue(args: List[PrimitiveValue]) = 
    FloatPrimitive((args(0).asDouble + args(1).asDouble) / 2.0)
  def lowerBound(args: List[PrimitiveValue]) = args(0)
  def upperBound(args: List[PrimitiveValue]) = args(1)
  def lowerBoundExpr(args: List[Expression]) = args(0)
  def upperBoundExpr(args: List[Expression]) = args(1)
  def sample(seed: Long, args: List[PrimitiveValue]) = {
    val low  = args(0).asDouble
    val high = args(1).asDouble
    FloatPrimitive(new Random(seed).nextDouble() * (high - low) + low)
  }
}

case class NoOpModel(vt: Type.T) extends SingleVarModel(vt) {
  def mostLikelyValue(args: List[PrimitiveValue]) = args(0)
  def lowerBound(args: List[PrimitiveValue]) = args(0)
  def upperBound(args: List[PrimitiveValue]) = args(0)
  def lowerBoundExpr(args: List[Expression]) = args(0)
  def upperBoundExpr(args: List[Expression]) = args(0)
  def sample(seed: Long, args: List[PrimitiveValue]) = args(0)
}