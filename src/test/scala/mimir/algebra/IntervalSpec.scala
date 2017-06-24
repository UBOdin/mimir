package mimir.algebra

import org.specs2.mutable._
import mimir.parser._;
import mimir.optimizer._;

object IntervalSpec extends Specification {

  def parser = new ExpressionParser((x: String) => null)
  def expr = parser.expr _


  "The Interval" should {

      "PLUSSECONDS" >> {
        Eval.eval(expr("PLUSSECONDS(5, 4)")) must be equalTo(IntervalPrimitive(4005))
      }

      "PLUSDAYS" >> {
        Eval.eval(expr("PLUSDAYS(50, 1)")) must be equalTo(IntervalPrimitive(86400050))
      }

      "TOHOURS" >> {
        Eval.eval(expr("TOHOURS(43200000)")) must be equalTo(FloatPrimitive(12))
      }

      "GETINTERVALS" >> {
        Eval.eval(expr("GETINTERVAL('2017-01-01','2017-01-02')")) must be equalTo(IntervalPrimitive(86400000))
      }

      "GETINTERVALS" >> {
        Eval.eval(expr("GETINTERVAL('2017-01-01T01:01:01Z','2017-01-02T01:01:02Z')")) must be equalTo(IntervalPrimitive(86401000))
      }


      "PLUSINTERVAL" >> {
        Eval.eval(expr("PLUSINTERVAL('2017-01-01T08:09:10Z',86401000)")) must be equalTo(TimestampPrimitive(2017,1,2,8,9,11))
      }


    }
}
