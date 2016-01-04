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
    PropagateConditions(expr(x))

  def booleanOpts(x:String) =
    FlattenBooleanConditionals(PullUpBranches(expr(x)))

  "Propagate Conditions" should {

    "Simplify Redundant Expressions" >> {
      conditionals("(A = 2) AND (A = 2)") must be equalTo expr("A = 2")
    }
    "Simplify Redundant Falsehoods" >> {
      conditionals("(A = 2) AND (A = 3)") must be equalTo expr("FALSE")
      conditionals("(A = 2) AND (A != 2)") must be equalTo expr("FALSE")
    }
    "Simplify If Statements" >> {
      conditionals("(A = 2) AND (IF A = 2 THEN 5 ELSE 6 END)") must be equalTo expr("(A = 2) AND 5")
      conditionals("(A = 3) AND (IF A = 2 THEN 5 ELSE 6 END)") must be equalTo expr("(A = 3) AND 6")
      conditionals("(A IS NULL) AND (IF A IS NULL THEN 5 ELSE 6 END)") must be equalTo expr("(A IS NULL) AND 5")
    }    
    "Simplify Negation" >> {
      conditionals("(A IS NOT NULL) AND (A IS NULL)") must be equalTo expr("FALSE")
      conditionals("(A IS NOT NULL) AND (IF A IS NULL THEN 5 ELSE 6 END)") must be equalTo expr("(A IS NOT NULL) AND 6")
    }
  }

  "PullUpBranches" should {
    "Simplify Arithmetic" >> {
      booleanOpts("(IF A = 1 THEN B ELSE C END) + 2") must be equalTo expr("""
        IF A = 1 THEN B + 2 ELSE C + 2 END
      """)
    }

    "Flatten Boolean Expressions" >> {
      booleanOpts("(IF A = 1 THEN B ELSE C END) = 2") must be equalTo expr("""
        ((A = 1) AND (B = 2)) OR ((NOT (A = 1)) AND (C = 2))
      """)
    }
  }
}