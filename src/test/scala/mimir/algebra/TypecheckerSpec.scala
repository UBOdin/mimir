package mimir.algebra;

import org.specs2.mutable._

import mimir.parser._

object TypecheckerSpec extends Specification {
  
  def parser = new ExpressionParser(null)
  def expr = parser.expr _
  def check(e: String) = Typechecker.typeOf(expr(e))
  def checkWeak(e: String) = Typechecker.weakTypeOf(expr(e))

  "The Typechecker" should {
  	"Infer Conditional Types Correctly" >> {
  		check("IF TRUE THEN 1 ELSE 2 END") must be equalTo Type.TInt
  		check("IF TRUE THEN TRUE ELSE FALSE END") must be equalTo Type.TBool
  		checkWeak("IF TRUE THEN B = 1 ELSE C = 1 END") must be equalTo Type.TBool
  		checkWeak("IF A = 1 THEN B = 1 ELSE C = 1 END") must be equalTo Type.TBool
  	}
  }

}