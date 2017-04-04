package mimir.test

import java.io._
import mimir.algebra._
import mimir.models._
import mimir.parser._

trait RAParsers {

  def modelLookup(model: String): Model
  def schemaLookup(table: String): Seq[(String,Type)]

  def parser = new OperatorParser(modelLookup _, schemaLookup _)
  def expr(
    raw: String, 
    inlineExpression: Map[String,Expression] = Map()
  ): Expression =
  {
    val parsed = parser.expr(raw)
    if(inlineExpression.isEmpty){ return parsed; }
    else { return Eval.inlineWithoutSimplifying(parsed, inlineExpression) }
  }
  def oper(
    raw: String, 
    inlineExpression: Map[String, Expression] = Map()
  ): Operator =
  {
    var parsed = parser.operator(raw)
    if(!inlineExpression.isEmpty){
      parsed = 
        inlineOperExpressions(parsed, inlineExpression)
    }
    return parsed
  }
  def inlineOperExpressions(op: Operator, exprs: Map[String, Expression]): Operator =
  {
    op.
      recurExpressions(Eval.inlineWithoutSimplifying(_, exprs)).
      recur(inlineOperExpressions(_, exprs))
  }

  def i = IntPrimitive(_:Long).asInstanceOf[PrimitiveValue]
  def f = FloatPrimitive(_:Double).asInstanceOf[PrimitiveValue]
  def str = StringPrimitive(_:String).asInstanceOf[PrimitiveValue]

}