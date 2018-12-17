package mimir.algebra.typeregistry

import mimir.algebra._
import scala.util.matching.Regex
import mimir.util.TextUtils

sealed abstract class TypeConstraint
{
  def test(v: String): Boolean
  def tester(target:Expression): Expression
}

case class RegexpConstraint(matcher: Regex) extends TypeConstraint
{
  def test(v:String) = { (matcher findFirstMatchIn v) != None }
  def tester(target:Expression) = Function("rlike", Seq(target, StringPrimitive(matcher.regex)))
}

case class EnumConstraint(values: Set[String], caseSensitive: Boolean = false) extends TypeConstraint
{
  lazy val caseSensitiveValues = 
    if(caseSensitive) { values } 
    else { values.map { _.toLowerCase } }

  def test(v:String) = 
    if(caseSensitive) { values contains v }
    else { values contains v.toLowerCase }
  def tester(target:Expression) = 
    ExpressionUtils.makeOr(
      caseSensitiveValues.map { v =>
        Comparison(Cmp.Eq, target, StringPrimitive(v))
      }
    )
  def base = TString()
}

case class IntExpressionConstraint(constraint: Expression, targetType: BaseType) extends TypeConstraint
{
  lazy val eval = new Eval()
  
  def test(v: String) = 
    eval.evalBool(constraint, Map(
      "x" -> TextUtils.parsePrimitive(targetType, v)
    ))
  def tester(target:Expression) =
    Eval.inline(constraint, Map(
      "x" -> target
    ))
}