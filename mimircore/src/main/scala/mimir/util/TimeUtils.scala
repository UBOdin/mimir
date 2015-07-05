package mimir.util;

import com.github.nscala_time.time.Imports._

object TimeUtils {
  def monitor[A](desc: String, op: Unit => A) : A = {
    val start = DateTime.now;
    val ret = op()
    val end = DateTime.now;
    println(desc + " took: " + ((start to end).millis)  + "ms")
    ret
  }
  
}