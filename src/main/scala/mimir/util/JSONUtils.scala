package mimir.util;

import play.api.libs.json._
import mimir.algebra._
import mimir.serialization.AlgebraJson._


object JsonUtils {

  val dotPrefix = "\\.([^.\\[]+).*".r
  val bracketPrefix = "\\[([0-9]+)\\].*".r

  def seekPath(jv: JsValue, path: String): JsValue =
  {
    if(jv.equals(JsNull)) { return JsNull; }
    path match {

      case "" => return jv;

      case dotPrefix(arg) => {
        val jo:JsObject = jv.as[JsObject]
        jo.value.get(arg) match {
          case Some(child) => seekPath(child, path.substring(arg.length + 1))
          case None => JsNull
        }
      }

      case bracketPrefix(arg) => {
        val ja:JsArray = jv.as[JsArray]
        val idx = Integer.parseInt(arg)
        if(ja.value.size > idx){
          seekPath(ja.value(idx), path.substring(arg.length + 2))
        } else {
          JsNull
        }
      }

      case _ =>
        throw new RAException(s"Invalid JSON Path Expression: '$path'")
    }
  }

}
