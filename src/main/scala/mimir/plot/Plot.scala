package mimir.plot

import com.typesafe.scalalogging.slf4j.LazyLogging

import vegas._
import vegas.render.WindowRenderer

import mimir.algebra._
import mimir.exec.stream._

/**
 * Starting to play around with using Mimir to replace my normal analytics workflows.
 * 
 * Since this is a hack for now, this method is invoked as a pragma:
 *   `PRAGMA PLOT(tableName, x, y)`
 * 
 * Trying out Vegas as a plotting engine, although unfortunately it doesn't quite work yet.
 * Calling plot.show only seems to generate a blank window.
 *
 * If someone wants to take a crack at getting this to work, it'd be great.
 *
 * It's possible that it's an issue that got fixed more recently: there's a slew of more recent
 * versions of Vegas... but they're all targetted at Scala 1.11, and porting all of Mimir to 1.11
 * is going to be a pretty serious undertaking.
 */
object Plot
  extends LazyLogging
{

  def value: (PrimitiveValue => Object) = {
    case NullPrimitive() => null
    case IntPrimitive(l) => l:java.lang.Long
    case FloatPrimitive(f) => f:java.lang.Double
    case StringPrimitive(s) => s
    case d:DatePrimitive => d.asDateTime
    case t:TimestampPrimitive => t.asDateTime
  }

  def plot(data: ResultIterator, title: String, x: String, y: String)
  {
    logger.debug(s"Plotting: $title")
    val values = 
      data.map { row => 
        logger.trace(s"Read: ${row.tuple}")
        (value(row(x)), value(row(y)))
      }.
      filter { case (x, y) => (x != null && y != null) }.
      map { row => Map(x -> row._1, y -> row._2) }.
      toSeq

    values.foreach { row => logger.debug(s"Plot: $row") }

    val chart = Vegas(title).
      withData(values:_*).
      encodeX(x, Quantitative).
      encodeY(y, Quantitative).
      mark(Line)
    
    (chart:WindowRenderer).show

  }
}