package mimir.util;

import com.github.nscala_time.time.Imports._

object TimeUtils {
  var globalStart:DateTime = null

  def mark(msg: String) = {
  	if(globalStart == null) { globalStart = DateTime.now }
  	val now = DateTime.now
  	println("> "+(globalStart to now).millis + "ms: "+msg)
  }


  def monitor[A](desc: String, op: Unit => A) : A = {
    val start = DateTime.now;
    val ret = op()
    val end = DateTime.now;
    println(desc + " took: " + ((start to end).millis)  + "ms")
    ret
  }

}