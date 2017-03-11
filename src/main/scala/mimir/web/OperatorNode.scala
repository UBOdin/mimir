package mimir.web

import scala.util.parsing.json.{JSONArray, JSONObject}

class OperatorNode(nodeName: String, c: Seq[OperatorNode], params: Option[String]) {
  val name = nodeName
  val children = c
  val args = params

  def toJson(): JSONObject =
    new JSONObject(Map("name" -> name,
                       "children" -> JSONArray(children.toList.map(a => a.toJson())),
                       "args" -> JSONArray(args.toList)))
}
