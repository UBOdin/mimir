package mimir.util;

import com.github.nscala_time.time.Imports._
import mimir.algebra._
import org.joda.time.{DateTime, Seconds, Days, Period}


object TimeUtils {
  var globalStart:DateTime = null
  var currLogger: String => Unit = null

  def mark(msg: String) = {
  	if(globalStart == null) { globalStart = DateTime.now }
  	val now = DateTime.now
  	currLogger("> "+(globalStart to now).millis + "ms: "+msg)
  }

  def monitor[A](desc: String, logger: String => Unit = println(_))(op: => A) : A = {
    val oldLogger = currLogger
    currLogger = logger
    val start = DateTime.now
    try {
      op
    } finally {
      val end = DateTime.now
      currLogger = oldLogger
      logger(s"$desc took: ${(start to end).millis} ms")
    }
  }

  def getDaysBetween(start: PrimitiveValue, end: PrimitiveValue): Long = {
    val startDT = start.asDateTime
    val endDT = end.asDateTime
    if(startDT.isBefore(endDT))  
      Days.daysBetween(startDT, endDT).getDays() 
    else
      Days.daysBetween(startDT, endDT).getDays() 
  }

  def getSecondsBetween(start: PrimitiveValue, end: PrimitiveValue): Long = {
    val startDT = start.asDateTime
    val endDT = end.asDateTime
    if(startDT.isBefore(endDT))
      Seconds.secondsBetween(startDT, endDT).getSeconds()
    else
      Seconds.secondsBetween(startDT, endDT).getSeconds()
  }

  
}