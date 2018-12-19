package mimir.parser

import org.specs2.mutable._

import mimir.algebra._

class ExpressionParserSpec extends Specification
{

  "ExpressionUtils (in possibly related matters)" >> 
  {
    "Extract from sequences of ORs" >> {
      ExpressionUtils.getDisjuncts(
        Var("A").or(
          Var("B").or(
            Var("C")
          )
        )
      ) must contain(allOf[Expression](Var("A"), Var("B"), Var("C")))
    }
  }

  "The Expression Parser" >> 
  {
    "Handle sequences of ORs" >> {
      ExpressionUtils.getDisjuncts(
        ExpressionParser.expr("(A) OR (B) OR (C)") 
      ) must contain(allOf[Expression](Var("A"), Var("B"), Var("C")))
    }
    "Handle sequences of ANDs" >> {
      ExpressionUtils.getConjuncts(
        ExpressionParser.expr("(A) AND (B) AND (C)") 
      ) must contain(allOf[Expression](Var("A"), Var("B"), Var("C")))
    }

  }

}
