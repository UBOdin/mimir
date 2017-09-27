package mimir.util;

import com.github.nscala_time.time.Imports._
import mimir.algebra._
import org.joda.time.{DateTime, Seconds, Days, Period}


object TimeUtils {

  def getDaysBetween(start: PrimitiveValue, end: PrimitiveValue): Long = 
  {
    val startDT = start.asDateTime
    val endDT = end.asDateTime
    if(startDT.isBefore(endDT))  
      Days.daysBetween(startDT, endDT).getDays() 
    else
      Days.daysBetween(startDT, endDT).getDays() 
  }

  def getSecondsBetween(start: PrimitiveValue, end: PrimitiveValue): Long = 
  {
    val startDT = start.asDateTime
    val endDT = end.asDateTime
    if(startDT.isBefore(endDT))
      Seconds.secondsBetween(startDT, endDT).getSeconds()
    else
      Seconds.secondsBetween(startDT, endDT).getSeconds()
  }

  
}