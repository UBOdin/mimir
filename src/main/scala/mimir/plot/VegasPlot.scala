package mimir.plot

import com.typesafe.scalalogging.slf4j.LazyLogging

import java.io.{File, FileWriter}

import vegas._
import vegas.DSL.SpecBuilder
import vegas.render.WindowRenderer._
// import vegas.render.HTMLRenderer._


import mimir.algebra._
import mimir.exec.result._

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
object VegasPlot
  extends LazyLogging
{

  def plot(data: ResultIterator, title: String, x: String, y: Seq[String])
  {
    logger.debug(s"Plotting: $title")
    val sch = data.schema.map { _._1 }
    val values = 
      data.flatMap { row => 
        logger.trace(s"Read: ${row.tuple}")
        y.map { c => Map( "X" -> Plot.value(row(x)), "Y" -> Plot.value(row(c)), "LINE" -> c ) }
      }.
      toSeq

    values.foreach { row => logger.debug(s"Plot: $row") }

    Vegas(title).
      withData(values).
      encodeX("X", Quantitative, title = x).
      encodeY("Y", Quantitative, title = y.mkString(", ")).
      mark(Line).
      encodeColor(field = "LINE", Ordinal).
      show
  }

  def test()
  {
    val plot = Vegas("Country Pop").
      withData(Seq(
          Map("country" -> "USA", "population" -> 314),
          Map("country" -> "UK", "population" -> 64),
          Map("country" -> "DK", "population" -> 80)
        )
      ).
      encodeX("country", Nominal).
      encodeY("population", Quantitative).
      mark(Bar)

    plot.show
  }
}