package mimir.algebra;

import java.io.{StringReader,FileReader}

import mimir.parser.{MimirJSqlParser}
import org.specs2.mutable._

import mimir._
import mimir.parser._
import mimir.algebra._
import mimir.sql._

object ExpressionOptimizerSpec extends Specification {
  
  def parser = new ExpressionParser(null)
  def expr = parser.expr _

  def conditionals(x:String) = 
    ExpressionOptimizer.propagateConditions(expr(x))

  "Propagate Conditions" should {

    "Simplify Redundant Expressions" in {
      conditionals("(A = 2) AND (A = 2)") must be equalTo expr("A = 2")
    }
    "Simplify Redundant Falsehoods" in {
      conditionals("(A = 2) AND (A = 3)") must be equalTo expr("FALSE")
    }
    "Simplify If Statements" in {
      conditionals("(A = 2) AND (CASE WHEN A = 2 THEN 5 ELSE 6 END)") must be equalTo expr("(A = 2) AND 5")
      conditionals("(A = 3) AND (CASE WHEN A = 2 THEN 5 ELSE 6 END)") must be equalTo expr("(A = 3) AND 6")
      conditionals("(A IS NULL) AND (CASE WHEN A IS NULL THEN 5 ELSE 6 END)") must be equalTo expr("(A IS NULL) AND 5")
    }    
    "Simplify Negation" in {
      conditionals("(A IS NOT NULL) AND (A IS NULL)") must be equalTo expr("FALSE")
      conditionals("(A IS NOT NULL) AND (CASE WHEN A IS NULL THEN 5 ELSE 6 END)") must be equalTo expr("(A IS NOT NULL) AND 6")
    }
  }
}