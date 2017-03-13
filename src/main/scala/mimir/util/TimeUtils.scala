package mimir.util;

import com.github.nscala_time.time.Imports._

object TimeUtils {
  var globalStart:DateTime = null
  var currLogger: String => Unit = null

  def mark(msg: String) = {
  	if(globalStart == null) { globalStart = DateTime.now }
  	val now = DateTime.now
  	currLogger("> "+(globalStart to now).millis + "ms: "+msg)
  }

  def monitor[A](desc: String, op: () => A, logger: String => Unit) : A = {
    val oldLogger = currLogger
    currLogger = logger
    val start = DateTime.now
    val ret = op()
    val end = DateTime.now
    currLogger = oldLogger
    logger(s"$desc took: ${(start to end).millis} ms")
    ret
  }

}