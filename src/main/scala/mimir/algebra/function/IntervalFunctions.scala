package mimir.algebra.function;

import mimir.algebra._
import org.joda.time.DateTime

object IntervalFunctions
{

  def register()
  {
    FunctionRegistry.registerNative("TOMILLISECONDS", 
      (x) => FloatPrimitive(x(0).asLong), 
      (_) => TInt()
    )

    FunctionRegistry.registerNative("TOSECONDS", 
      (x) => FloatPrimitive(x(0).asLong / 1000), 
      (_) => TInt()
    )


    FunctionRegistry.registerNative("TOMINUTES", 
      (x) => FloatPrimitive(x(0).asLong /1000 / 60), 
      (_) => TInt()
    )

    FunctionRegistry.registerNative("TOHOURS", 
      (x) => FloatPrimitive(x(0).asLong /1000 / 60 / 60), 
      (_) => TInt()
    )

    FunctionRegistry.registerNative("TODAYS", 
      (x) => FloatPrimitive(x(0).asLong /1000 / 60 / 60 / 24), 
      (_) => TInt()
    )

    FunctionRegistry.registerNative("PLUSMILLISECONDS", 
      (x) => IntervalPrimitive(x(0).asLong + x(1).asLong), 
      (_) => TInt()
    )

    FunctionRegistry.registerNative("PLUSSECONDS", 
      (x) => IntervalPrimitive(x(0).asLong + x(1).asLong * 1000), 
      (_) => TInt()
    )

    FunctionRegistry.registerNative("PLUSMINUTES", 
      (x) => IntervalPrimitive(x(0).asLong + x(1).asLong * 1000 * 60), 
      (_) => TInt()
    )

    FunctionRegistry.registerNative("PLUSHOURS", 
      (x) => IntervalPrimitive(x(0).asLong + x(1).asLong * 1000 * 60 * 60), 
      (_) => TInt()
    )

    FunctionRegistry.registerNative("PLUSDAYS", 
      (x) => IntervalPrimitive(x(0).asLong + x(1).asLong * 1000 * 60 * 60 * 24), 
      (_) => TInt()
    )


    FunctionRegistry.registerNative(
      "GETINTERVAL",
      (x) => {
        IntervalPrimitive(Math.abs(x(0).asDateTime.getMillis - x(1).asDateTime.getMillis))
      },
      (_) => TInt()
    )

    FunctionRegistry.registerNative(
      "PLUSINTERVAL",
      (x) => {
	val d = x(0).asDateTime.plusMillis(java.lang.Integer.parseInt(x(1).asString))
        TimestampPrimitive(d.getYear,d.getMonthOfYear,d.getDayOfMonth,d.getHourOfDay,d.getMinuteOfHour,d.getSecondOfMinute)
      },
      (_) => TInt()
    )

  }

}
