package mimir.algebra;

import org.specs2.mutable._

import mimir.parser._

object TypecheckerSpec extends Specification {
  
  def typechecker = new Typechecker
  def expr = ExpressionParser.expr _
  def check(e: String) = typechecker.typeOf(expr(e))
  def checkWeak(e: String) = typechecker.weakTypeOf(expr(e))

  "The Typechecker" should {
  	"Infer Conditional Types Correctly" >> {
  		check("CASE WHEN TRUE THEN 1 ELSE 2 END") must be equalTo TInt()
  		check("CASE WHEN TRUE THEN TRUE ELSE FALSE END") must be equalTo TBool()
  		checkWeak("CASE WHEN TRUE THEN B = 1 ELSE C = 1 END") must be equalTo TBool()
  		checkWeak("CASE WHEN A = 1 THEN B = 1 ELSE C = 1 END") must be equalTo TBool()
  	}
  }

}