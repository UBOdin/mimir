package mimir.util;

import java.util.Set

import com.github.wnameless.json.flattener.JsonFlattener
import play.api.libs.json._
import mimir.algebra._
import scala.collection.JavaConverters._

import scala.util.parsing.json.JSONObject

object JsonUtils {

  val dotPrefix = "\\.([^.\\[]+)".r
  val bracketPrefix = "\\[([0-9]+)\\]".r

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

  // remove unwanted characters from a json string
  def JsonClean(s: String): String = {
    // returns a cleaned json object hopefully
    var clean = s.replace("\\\\", "") // clean the string of variables that will throw off parsing
    clean = clean.replace("\\\"", "")
    clean = clean.replace("\\n", "")
    clean = clean.replace("\\r", "")
    clean = clean.replace("\n", "")
    clean = clean.replace("\r", "")
    while (clean.charAt(0) != '{' || clean.charAt(clean.size - 1) != '}'){ // give it the best shot at being in json format
      if(clean.charAt(0) != '{')
        clean = clean.substring(1,clean.size)
      if (clean.charAt(clean.size - 1) != '}')
        clean = clean.substring(0,clean.size - 1)
    }
    return clean
  }

}
