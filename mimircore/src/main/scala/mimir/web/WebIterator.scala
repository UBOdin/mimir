package mimir.web

/**
 * Encapsulates query results into an easy to process
 * package for the web iterator
 *
 * Fields have been chosen to make the job of displaying
 * it in html through the templating language, easier
 *
 *    1. header - List of attribute names
 *    2. data - List of Tuple2 (Scala tuples)
 *              Tuple2._1 is a list of string values,
 *              representing row data
 *
 *              Tuple2._2 is a boolean indicating if
 *              that row is non-determinisitic
 *
 *    3. missingRows - Whether or not the result may have
 *              rows missing
 */

class WebIterator(h: List[String],
                  d: List[(List[String], Boolean)],
                  mR: Boolean,
                  eT: Double) {

  val header = h
  val data = d
  val missingRows = mR
  var executionTimeMS: Double = eT
  var queryFlow: OperatorNode = null

}
