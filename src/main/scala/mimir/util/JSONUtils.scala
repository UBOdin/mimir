package mimir.util;

import play.api.libs.json._
import mimir.algebra._
import mimir.serialization.Json


object JsonUtils {

  val dotPrefix = "\\.([^.\\[]+).*".r
  val bracketPrefix = "\\[([0-9]+)\\].*".r

  implicit val primitiveValueWrites = new Writes[PrimitiveValue] {
    def writes(p: PrimitiveValue): JsValue = 
    {
      Json.ofPrimitive(p)
    }
  }

  def seekPath(jv: JsValue, path: String): JsValue =
  {
    path match {

      case "" => return jv;

      case dotPrefix(arg) => {
        val jo:JsObject = jv.as[JsObject]
        seekPath(jo.value(arg), path.substring(arg.length + 1))
      }

      case bracketPrefix(arg) => {
        val ja:JsArray = jv.as[JsArray]
        seekPath(ja.value(Integer.parseInt(arg)), path.substring(arg.length + 2))
      }

      case _ =>
        throw new RAException(s"Invalid JSON Path Expression: '$path'")
    }
  }

}
