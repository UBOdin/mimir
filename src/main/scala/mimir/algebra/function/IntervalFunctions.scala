package mimir.algebra.function;

import mimir.algebra._
import org.joda.time.DateTime
import org.joda.time.Period;

object IntervalFunctions
{

  def register(fr: FunctionRegistry)
  {



    fr.register(
      "INTERVALPLUSINTERVAL",
      (x) => {
	var p = x(0).asInterval.plus(x(1).asInterval);
	IntervalPrimitive(p.getYears,p.getMonths,p.getWeeks,p.getDays,p.getHours,p.getMinutes,p.getSeconds,p.getMillis)
      },
      (_) => TInt()
    )


    fr.register(
      "INTERVALSUBINTERVAL",
      (x) => {
	var p = x(0).asInterval.minus(x(1).asInterval);
	IntervalPrimitive(p.getYears,p.getMonths,p.getWeeks,p.getDays,p.getHours,p.getMinutes,p.getSeconds,p.getMillis)
      },
      (_) => TInt()
    )

    fr.register(
      "DATEPLUSINTERVAL",
      (x) => {
	val d = x(0).asDateTime.plus(x(1).asInterval)
        TimestampPrimitive(d.getYear,d.getMonthOfYear,d.getDayOfMonth,d.getHourOfDay,d.getMinuteOfHour,d.getSecondOfMinute,d. getMillisOfSecond)
      },
      (_) => TInt()
    )

    fr.register(
      "DATESUBINTERVAL",
      (x) => {
	val d = x(0).asDateTime.minus(x(1).asInterval)
        TimestampPrimitive(d.getYear,d.getMonthOfYear,d.getDayOfMonth,d.getHourOfDay,d.getMinuteOfHour,d.getSecondOfMinute,d. getMillisOfSecond)
      },
      (_) => TInt()
    )


    fr.register(
      "DATESUBDATE",
      (x) => {
	val p = new Period(x(1).asDateTime,x(0).asDateTime)
        IntervalPrimitive(p.getYears,p.getMonths,p.getWeeks,p.getDays,p.getHours,p.getMinutes,p.getSeconds,p.getMillis)
      },
      (_) => TInt()
    )


/*
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
	val d = x(0).asDateTime.plusMillis(x(1).asLong.toInt)
        TimestampPrimitive(d.getYear,d.getMonthOfYear,d.getDayOfMonth,d.getHourOfDay,d.getMinuteOfHour,d.getSecondOfMinute)
      },
      (_) => TInt()
    )

    FunctionRegistry.registerNative(
      "SUBINTERVAL",
      (x) => {
	val d = x(0).asDateTime.plusMillis(-x(1).asLong.toInt)
        TimestampPrimitive(d.getYear,d.getMonthOfYear,d.getDayOfMonth,d.getHourOfDay,d.getMinuteOfHour,d.getSecondOfMinute)
      },
      (_) => TInt()
    )
*/
  }

}
