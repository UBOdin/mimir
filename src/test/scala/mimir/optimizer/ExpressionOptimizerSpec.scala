package mimir.optimizer;

import java.io.{StringReader,FileReader}

import org.specs2.mutable._

import mimir._
import mimir.parser._
import mimir.algebra._
import mimir.sql._
import mimir.test.RASimplify
import mimir.optimizer.expression._
import mimir.optimizer.operator._

object ExpressionOptimizerSpec 
  extends Specification 
  with RASimplify
{
  
  def typechecker = new Typechecker  
  def expr = ExpressionParser.expr _

  def conditionals(x:String) = 
    simplify(PropagateConditions(expr(x)))

  def booleanOpts(x:String) =
    new FlattenBooleanConditionals(typechecker)(PullUpBranches(expr(x)))

  "Propagate Conditions" should {

    "Simplify Redundant Expressions" >> {
      conditionals("((A = 2) AND (A = 2))") must be equalTo expr("A = 2")
    }
    "Simplify Redundant Falsehoods" >> {
      conditionals("((A = 2) AND (A = 3))") must be equalTo expr("FALSE")
      conditionals("((A = 2) AND (A != 2))") must be equalTo expr("FALSE")
    }
    "Simplify If Statements" >> {
      conditionals("((A = 2) AND (CASE WHEN A = 2 THEN 5 ELSE 6 END))") must be equalTo expr("(A = 2) AND 5")
      conditionals("((A = 3) AND (CASE WHEN A = 2 THEN 5 ELSE 6 END))") must be equalTo expr("(A = 3) AND 6")
      conditionals("((A IS NULL) AND (CASE WHEN A IS NULL THEN 5 ELSE 6 END))") must be equalTo expr("(A IS NULL) AND 5")
    }    
    "Simplify Negation" >> {
      conditionals("((A IS NOT NULL) AND (A IS NULL))") must be equalTo expr("FALSE")
      conditionals("((A IS NOT NULL) AND (CASE WHEN A IS NULL THEN 5 ELSE 6 END))") must be equalTo expr("(A IS NOT NULL) AND 6")
    }
  }

  "PullUpBranches" should {
    "Simplify Arithmetic" >> {
      booleanOpts("(CASE WHEN A = 1 THEN B ELSE C END) + 2") must be equalTo expr("""
        CASE WHEN A = 1 THEN B + 2 ELSE C + 2 END
      """)
    }

    "Flatten Boolean Expressions" >> {
      booleanOpts("(CASE WHEN A = 1 THEN B ELSE C END) = 2") must be equalTo expr("""
        ((A = 1) AND (B = 2)) OR ((A != 1) AND (C = 2))
      """)
    }
  }
}