package mimir.algebra

import org.specs2.mutable._
import mimir.parser._;

object EvalSpec extends Specification {

  def parser = new ExpressionParser((x: String) => null)
  def expr = parser.expr _
  def simplify(x: String) = Eval.simplify(expr(x))

  "The Evaluator" should {
    "not insert spurious nulls" >> {
      simplify("NOT ( (TRUE=TRUE) ) AND  (NULL>3)") must be equalTo expr("FALSE")
      simplify("""
        NOT( (FALSE=TRUE) ) AND  ( (TRUE=TRUE)  AND  (CAST('20', real)>3) )
      """) must be equalTo expr("TRUE")
      simplify("""
        NOT( (FALSE=TRUE) ) AND  ( (TRUE=TRUE)  AND  (CAST(RATINGS2_NUM_RATINGS, real)>3) )
      """) must be equalTo expr("CAST(RATINGS2_NUM_RATINGS, real)>3")
      Eval.simplify(
        Arithmetic(Arith.And,
          Not(expr("TRUE=TRUE")),
          Comparison(Cmp.Gt, NullPrimitive(), IntPrimitive(3))
        )
      ) must be equalTo BoolPrimitive(false)
      Eval.simplify(
        Arithmetic(Arith.And,
          Not(expr("FALSE=TRUE")),
          Arithmetic(Arith.Or,
            Arithmetic(Arith.And,
              expr("TRUE=TRUE"),
              expr("CAST(RATINGS2_NUM_RATINGS, real)>3")
            ),
            Arithmetic(Arith.And,
              Not(expr("TRUE=TRUE")),
              Comparison(Cmp.Gt, NullPrimitive(), IntPrimitive(3))
            )
          )
        )
      ) must be equalTo expr("CAST(RATINGS2_NUM_RATINGS, real)>3")
    }

  }



}