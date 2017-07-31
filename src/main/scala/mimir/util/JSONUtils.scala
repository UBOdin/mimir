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

  def jsonMerge(json1: String, json2: String): String = {
    // merge two json objects, this is a module made for the JSON_Explorer so it may be added to later
    // this is expecting a map essentially containing type information and possibly other details such as count, max, min

    var jsonMapCombined: Map[String, Any] = Map[String, Any]()

    try {
      val jsonMap1: java.util.Map[String,AnyRef] = JsonFlattener.flattenAsMap(json1)
      val jsonMap2: java.util.Map[String,AnyRef] = JsonFlattener.flattenAsMap(json1)
      val jsonMapKeySet: Set[String] = jsonMap1.keySet()
      for (key:String <- jsonMapKeySet.asScala){ // iterate through each key, for each key find it's type
        val jsonValue: String = jsonMap1.get(key).toString
        key.toLowerCase() match {
          case ("real" | "varchar" | "int") => {
            jsonMapCombined.get(key) match {
              case Some(v) => jsonMapCombined = jsonMapCombined + (key -> (jsonValue.toInt + v.toString.toInt))
              case None => jsonMapCombined = jsonMapCombined + (key -> jsonValue.toInt)
            }
          }
          case ("max") => ???
        }
      }
    }
    catch{
      case e: Exception => throw new Exception("JsonMerge trying to merge two objects and at least one is not json")
    }
    JSONObject(jsonMapCombined).toString()
  }

}
