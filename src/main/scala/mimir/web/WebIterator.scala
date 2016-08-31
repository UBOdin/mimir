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

class WebIterator(val header: List[String],
                  val data: List[(List[String], Boolean)],
                  val count: Long,
                  val missingRows: Boolean,
                  val executionTime: Double) {
  var queryFlow: OperatorNode = null

}
