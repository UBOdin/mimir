package mimir.web

import scala.util.parsing.json.JSONObject

/**
 * WebErrorResult encapsulates an error message,
 * this is useful for passing exception messages to
 * the web interface
 */

case class WebErrorResult(string: String) extends WebResult {
  val result = string

  def toJson() = {
    new JSONObject(Map("error" -> result))
  }
}
