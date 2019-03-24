package mimir.parser

import org.specs2.mutable._

import mimir.algebra._

class ExpressionParserSpec extends Specification
{

  "ExpressionUtils (in possibly related matters)" >> 
  {
    "Extract from sequences of ORs" >> {
      ExpressionUtils.getDisjuncts(
        Var(ID("A")).or(
          Var(ID("B")).or(
            Var(ID("C"))
          )
        )
      ) must contain(allOf[Expression](Var(ID("A")), Var(ID("B")), Var(ID("C"))))
    }
  }

  "The Expression Parser" >> 
  {
    "Handle sequences of ORs" >> {
      ExpressionUtils.getDisjuncts(
        ExpressionParser.expr("(A) OR (B) OR (C)") 
      ) must contain(allOf[Expression](Var(ID("A")), Var(ID("B")), Var(ID("C"))))
    }
    "Handle sequences of ANDs" >> {
      ExpressionUtils.getConjuncts(
        ExpressionParser.expr("(A) AND (B) AND (C)") 
      ) must contain(allOf[Expression](Var(ID("A")), Var(ID("B")), Var(ID("C"))))
    }

  }

}
