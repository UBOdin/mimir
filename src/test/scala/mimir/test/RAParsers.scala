package mimir.test

import java.io._
import mimir.algebra._
import mimir.models._
import mimir.parser._

trait RAParsers {

  def expr(
    raw: String, 
    inlineExpression: Map[String,Expression] = Map()
  ): Expression =
  {
    val parsed = ExpressionParser.expr(raw)
    if(inlineExpression.isEmpty){ return parsed; }
    else { return Eval.inline(parsed, inlineExpression) }
  }

  def i = IntPrimitive(_:Long).asInstanceOf[PrimitiveValue]
  def f = FloatPrimitive(_:Double).asInstanceOf[PrimitiveValue]
  def str = StringPrimitive(_:String).asInstanceOf[PrimitiveValue]

}