package mimir.web

import scala.util.parsing.json.JSONObject

/**
 * WebQueryResult encapsulates a WebIterator, this is useful
 * for packaging query results
 */

case class WebQueryResult(webIterator: WebIterator) extends WebResult {
  val result = webIterator

  def toJson() = {
    new JSONObject(Map("header" -> result.header,
                        "data" -> result.data,
                        "missingRows" -> result.missingRows,
                        "queryFlow" -> result.queryFlow.toJson()))
  }
}
