package mimir.web

import scala.util.parsing.json.JSONObject

/**
 * WebStringResult just encapsulates String responses,
 * like for EXPLAIN queries
 */

case class WebStringResult(string: String) extends WebResult {
  val result = string

  def toJson() = {
    new JSONObject(Map("result" -> result))
  }
}
