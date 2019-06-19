package mimir.algebra

import org.specs2.mutable._
import mimir.parser._
import mimir.algebra._
import mimir.algebra.function.FunctionRegistry
import mimir.optimizer._
import mimir.optimizer.expression._
import mimir.test._

object EvalSpec extends Specification with RASimplify {

  def expr = ExpressionParser.expr _

  "The Evaluator" should {

    "evaluate comparisons properly" >> {
      simplify("20.0>3") must be equalTo expr("TRUE")
      simplify("CAST('20', real)>3") must be equalTo expr("TRUE")
    }

    "not insert spurious nulls" >> {
      simplify("NOT ( (TRUE=TRUE) ) AND  (NULL>3)") must be equalTo expr("FALSE")
      simplify("""
        NOT( (FALSE=TRUE) ) AND  ( (TRUE=TRUE)  AND  (CAST('20', real)>3) )
      """) must be equalTo expr("TRUE")
      simplify("""
        NOT( (FALSE=TRUE) ) AND  ( (TRUE=TRUE)  AND  (CAST(RATINGS2_NUM_RATINGS, real)>3) )
      """) must be equalTo expr("CAST(RATINGS2_NUM_RATINGS, real)>3")
      simplify(
        Arithmetic(Arith.And,
          Not(expr("TRUE=TRUE")),
          Comparison(Cmp.Gt, NullPrimitive(), IntPrimitive(3))
        )
      ) must be equalTo BoolPrimitive(false)
      simplify(
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

    "support date comparisons" >> {
      Eval.applyCmp(Cmp.Gte, DatePrimitive(2017, 12, 1), DatePrimitive(2017, 9, 2)).asBool must beTrue
      Eval.applyCmp(Cmp.Lt, DatePrimitive(2017, 12, 1), DatePrimitive(2017, 9, 2)).asBool must beFalse
    }

    "support timestamp comparisons" >> {
      Eval.applyCmp(Cmp.Gt, TimestampPrimitive(2017, 12, 1, 0, 0, 0, 0), TimestampPrimitive(2017, 9, 2, 0, 0, 0, 0)).asBool must beTrue
      Eval.applyCmp(Cmp.Lte, TimestampPrimitive(2017, 12, 1, 0, 0, 0, 0), TimestampPrimitive(2017, 9, 2, 0, 0, 0, 0)).asBool must beFalse
    }

  }

  "The Inliner" should {

    "Properly expand DISTANCE" >> {
      simplify("DISTANCE(3, 4)") must be equalTo(FloatPrimitive(5))
      simplify("DISTANCE(Q, R)") must be equalTo(
        Function(ID("sqrt"), List(
          Arithmetic(Arith.Add,
            Arithmetic(Arith.Mult, Var(ID("Q")), Var(ID("Q"))),
            Arithmetic(Arith.Mult, Var(ID("R")), Var(ID("R")))
          )
      ))
      )
    }

  }



}