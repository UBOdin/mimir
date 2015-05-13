package mimir.ctables

import scala.util._

import mimir.Database
import mimir.algebra._

abstract class SingleVarModel(vt: Type.T) extends Model {
  def varType = vt
  def varTypes = List(vt)

  def mostLikelyValue(args: List[PrimitiveValue]): PrimitiveValue;
  def boundsValues(args: List[PrimitiveValue]): (PrimitiveValue, PrimitiveValue)
  def boundsExpressions(args: List[Expression    ]): (Expression, Expression)
  def sample(seed: Long, args: List[PrimitiveValue]):  PrimitiveValue

  def mostLikelyValue(x:Int, args: List[PrimitiveValue]) = mostLikelyValue(args);
  def boundsValues(x: Int, args: List[PrimitiveValue]) = boundsValues(args);
  def boundsExpressions(x: Int, args: List[Expression]) = boundsExpressions(args);
  def sample(seed: Long, x: Int, args: List[PrimitiveValue]) = sample(seed,args);
}

case class JointSingleVarModel(vars: List[SingleVarModel]) extends Model {
  def varTypes = vars.map( _.varType )

  def mostLikelyValue  (idx: Int, args: List[PrimitiveValue]) = 
    vars(idx).mostLikelyValue(args);
  def boundsValues     (idx: Int, args: List[PrimitiveValue]) = 
    vars(idx).boundsValues(args);
  def boundsExpressions(idx: Int, args: List[Expression    ]) = 
    vars(idx).boundsExpressions(args);
  def sample(seed: Long, idx: Int, args: List[PrimitiveValue]) = 
    vars(idx).sample(seed, args);
}

object UniformDistribution extends SingleVarModel(Type.TFloat){
  def mostLikelyValue(args: List[PrimitiveValue]) = 
    FloatPrimitive((args(0).asDouble + args(1).asDouble) / 2.0)
  def boundsValues(args: List[PrimitiveValue]) = (args(0), args(1))
  def boundsExpressions(args: List[Expression]) = (args(0), args(1))
  def sample(seed: Long, args: List[PrimitiveValue]) = {
    val low  = args(0).asDouble
    val high = args(1).asDouble
    FloatPrimitive(new Random(seed).nextDouble() * (high - low) + low)
  }
}