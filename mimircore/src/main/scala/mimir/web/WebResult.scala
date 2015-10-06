package mimir.web

import scala.util.parsing.json.JSONObject

/**
 * WebResult is an
 */

abstract class WebResult {
  def toJson(): JSONObject
}
