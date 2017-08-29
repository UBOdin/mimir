package mimir.algebra

import org.specs2.mutable._
import mimir.parser._
import mimir.algebra._
import mimir.algebra.function.FunctionRegistry
import mimir.optimizer._
import mimir.optimizer.expression._
import mimir.test._

object IntervalSpec extends Specification with RASimplify {


  def expr = ExpressionParser.expr _


  "The Interval" should {
      "Parse Interval" >> {
	simplify("INTERVAL('P1Y2M3W4DT5H6M7.008S')") must be equalTo(IntervalPrimitive(new org.joda.time.Period(1,2,3,4,5,6,7,8)))
      }

      "Interval + Interval" >> {
        simplify("INTERVAL('P1Y2M3W4DT5H6M7.008S')+INTERVAL('P0Y1M1W1DT0H0M0S')") must be equalTo(IntervalPrimitive(new org.joda.time.Period(1,3,4,5,5,6,7,8)))
      }

      "Interval - Interval" >> {
        simplify("INTERVAL('P1Y3M4W5DT5H6M7.008S')-INTERVAL('P0Y1M1W1DT0H0M0S')") must be equalTo(IntervalPrimitive(new org.joda.time.Period(1,2,3,4,5,6,7,8)))
      }

      "Date + Interval" >> {
        simplify("DATE('2017-01-01')+INTERVAL('P0Y1M1W1DT0H0M0S')") must be equalTo(TimestampPrimitive(2017,2,9,0,0,0,0))
      }

      "Date - Interval" >> {
        simplify("DATE('2017-02-09')-INTERVAL('P0Y1M1W1DT0H0M0S')") must be equalTo(TimestampPrimitive(2017,1,1,0,0,0,0))
      }

      "Date - Date" >> {
        simplify("DATE('2017-05-01')-DATE('2017-01-01')") must be equalTo(IntervalPrimitive(new org.joda.time.Period(0,4,0,0,0,0,0,0)))
      }

      "Interval * Scalar" >> {
        simplify("INTERVAL('P0Y0M0W0DT4H3M2.001S')*2") must be equalTo(IntervalPrimitive(new org.joda.time.Period(0,0,0,0,8,6,4,2)))
        simplify("2*INTERVAL('P0Y0M0W0DT4H3M2.001S')") must be equalTo(IntervalPrimitive(new org.joda.time.Period(0,0,0,0,8,6,4,2)))
      }



    }



}

