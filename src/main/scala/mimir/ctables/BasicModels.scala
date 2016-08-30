package mimir.ctables

import mimir.algebra._

import scala.util._

abstract class SingleVarModel() extends Model {

  val numVars = 1
  def varType(argTypes: List[Type.T]): Type.T
  def bestGuess(args: List[PrimitiveValue]): PrimitiveValue
  def sample(randomness: Random, args: List[PrimitiveValue]): PrimitiveValue
  def reason(args: List[Expression]): String

  def varType(x: Int, argTypes: List[Type.T]): Type.T = 
    varType(argTypes)
  def bestGuess(x:Int, args: List[PrimitiveValue]): PrimitiveValue =
    bestGuess(args)
  def sample(x:Int, randomness: Random, args: List[PrimitiveValue]): PrimitiveValue =
    sample(randomness, args)
  def reason(x:Int, args: List[Expression]): String =
    reason(args)
}

case class IndependentVarsModel(vars: List[SingleVarModel]) extends Model {

  val numVars = 1
  def varType(idx: Int, argTypes: List[Type.T]) = 
    vars(idx).varType(argTypes)
  def bestGuess(idx: Int, args: List[PrimitiveValue]) = 
    vars(idx).bestGuess(args);
  def sample(idx: Int, randomness: Random, args: List[PrimitiveValue]) =
    vars(idx).sample(randomness, args)
  def reason(idx: Int, args: List[Expression]): String =
    vars(idx).reason(idx, args)
}

case class MergedModels(models: List[Model]) extends Model
{
  val varOffsetByModel = List(0)++models.map(_.numVars).scan(0)(_+_)
  val modelByVarIdx = 
    models.
    zipWithIndex.
    flatMap({ case (model, idx) => 
      0.to(model.numVars).map( _ => idx )
    })
  
  val numVars = models.map( _.numVars ).sum
  def varType(idx: Int, argTypes:List[Type.T]): Type.T = {
    val modelIdx = modelByVarIdx(idx)
    models(modelIdx).varType(idx-varOffsetByModel(modelIdx), argTypes)
  }
  def bestGuess(idx: Int, args: List[PrimitiveValue]):  PrimitiveValue = {
    val modelIdx = modelByVarIdx(idx)
    models(modelIdx).bestGuess(idx-varOffsetByModel(modelIdx), args)
  }
  def sample(idx: Int, randomness: Random, args: List[PrimitiveValue]):  PrimitiveValue = {
    val modelIdx = modelByVarIdx(idx)
    models(modelIdx).sample(idx-varOffsetByModel(modelIdx), randomness, args)
  }
  def reason(idx: Int, args: List[Expression]): String = {
    val modelIdx = modelByVarIdx(idx)
    models(modelIdx).reason(idx-varOffsetByModel(modelIdx), args)
  }
}

object UniformDistribution extends SingleVarModel(){
  def varType(argTypes: List[Type.T]) = Type.TFloat
  def bestGuess(args: List[PrimitiveValue]) = 
    FloatPrimitive((args(0).asDouble + args(1).asDouble) / 2.0)
  def sample(randomness: Random, args: List[PrimitiveValue]) = {
    val low = args(0).asDouble
    val high = args(1).asDouble
    FloatPrimitive(
      (randomness.nextDouble() * (high - low)) + high
    )
  }
  def bestGuessExpr(args: List[Expression]) = 
    Arithmetic(Arith.Div,
      Arithmetic(Arith.Add, args(0), args(1)),
      FloatPrimitive(2.0)
    )
  def sampleExpr(randomness: Expression, args: List[Expression]) = 
    Arithmetic(Arith.Add,
      Arithmetic(Arith.Mult,
        randomness,
        Arithmetic(Arith.Sub, args(1), args(0))
      ),
      args(0)
    )


  def reason(args: List[Expression]): String = 
    "I put in a random value between "+args(0)+" and "+args(1)
}


case class NoOpModel(vt: Type.T) extends SingleVarModel() {
  def varType(argTypes: List[Type.T]) = vt
  def bestGuess(args: List[PrimitiveValue]) = args(0)
  def sample(randomness: Random, args: List[PrimitiveValue]) = args(0)
  def reason(args: List[Expression]): String = 
    "I was asked to tag this value for some reason"
}