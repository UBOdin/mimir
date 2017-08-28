package mimir.algebra.function;

import org.geotools.referencing.datum.DefaultEllipsoid
import org.joda.time.DateTime
import mimir.algebra._

object TimeFunctions
{
  def register(fr: FunctionRegistry)
  {


    fr.registerNative("YEAR_PART", 
      { 
        case Seq(TimestampPrimitive()) => 
        case Seq(DatePrimitive()) => 
        case Seq(IntervalPrimitive()) => 
        case x => throw new Exception(s"Invalid Time Primitive '$x'")
      }
    )
    fr.registerNative("MONTH_PART", 
      { 
        case Seq(TimestampPrimitive()) => 
        case Seq(DatePrimitive()) => 
        case Seq(IntervalPrimitive()) => 
        case x => throw new Exception(s"Invalid Time Primitive '$x'")
      }
    )
    fr.registerNative("DAY_PART", 
      { 
        case Seq(TimestampPrimitive()) => 
        case Seq(DatePrimitive()) => 
        case Seq(IntervalPrimitive()) => 
        case x => throw new Exception(s"Invalid Time Primitive '$x'")
      }
    )
    fr.registerNative("HOUR_PART", 
      { 
        case Seq(TimestampPrimitive()) => 
        case Seq(DatePrimitive()) => 
        case Seq(IntervalPrimitive()) => 
        case x => throw new Exception(s"Invalid Time Primitive '$x'")
      }
    )
    fr.registerNative("MINUTE_PART", 
      { 
        case Seq(TimestampPrimitive()) => 
        case Seq(DatePrimitive()) => 
        case Seq(IntervalPrimitive()) => 
        case x => throw new Exception(s"Invalid Time Primitive '$x'")
      }
    )
    fr.registerNative("SECOND_PART", 
      { 
        case Seq(TimestampPrimitive()) => 
        case Seq(DatePrimitive()) => 
        case Seq(IntervalPrimitive()) => 
        case x => throw new Exception(s"Invalid Time Primitive '$x'")
      }
    )
    fr.registerNative("MILLISECOND_PART", 
      { 
        case Seq(TimestampPrimitive()) => 
        case Seq(DatePrimitive()) => 
        case Seq(IntervalPrimitive()) => 
        case x => throw new Exception(s"Invalid Time Primitive '$x'")
      }
    )


    fr.registerExpr("DISTANCE", List("A", "B"), 
      Function("SQRT", List(
        Arithmetic(Arith.Add,
          Arithmetic(Arith.Mult, Var("A"), Var("A")),
          Arithmetic(Arith.Mult, Var("B"), Var("B"))
      ))))

  }
}