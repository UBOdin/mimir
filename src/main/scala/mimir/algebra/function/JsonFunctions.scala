package mimir.algebra.function;

import play.api.libs.json._
import mimir.algebra._
import mimir.util._

object JsonFunctions
{
  def extract(args: Seq[PrimitiveValue]): JsValue =
  {
    args match { 
      case Seq(text, path) => extract(text.asString, path.asString)
    }
  }

  def extract(args: Seq[PrimitiveValue], t: Type): PrimitiveValue =
  {
    JsonUtils.parsePrimitive(t, extract(args))
  }

  def extractAny(args: Seq[PrimitiveValue]): PrimitiveValue =
  {
    StringPrimitive(extract(args).toString)
  }

  def extract(text: String, path: String): JsValue =
  {
    val json = Json.parse(text)
    if(path(0) != '$'){
      throw new RAException(s"Invalid Path String '$path'")
    }
    JsonUtils.seekPath(json, path.substring(1))
  }

  def register()
  {
    FunctionRegistry.registerNative("JSON_EXTRACT", extractAny(_), (_) => TString())
    FunctionRegistry.registerNative("JSON_EXTRACT_INT", extract(_, TInt()), (_) => TInt())
    FunctionRegistry.registerNative("JSON_EXTRACT_FLOAT", extract(_, TFloat()), (_) => TFloat())
    FunctionRegistry.registerNative("JSON_EXTRACT_STR", extract(_, TString()), (_) => TString())
    FunctionRegistry.registerNative("JSON_ARRAY",
      (params: Seq[PrimitiveValue]) => 
        StringPrimitive(
          JsArray(
            params.map( JsonUtils.toJson(_) )
          ).toString
        ),
      (_) => TString()
    )
    FunctionRegistry.registerNative("JSON_OBJECT", 
      (params: Seq[PrimitiveValue]) => {
        StringPrimitive(
          JsObject(
            params.grouped(2).map {
              case Seq(k, v) => (k.asString -> JsonUtils.toJson(v))
            }.toMap
          ).toString
        )
      },
      (_) => TString()
    )
    FunctionRegistry.registerNative("JSON_ARRAY_LENGTH",
      _ match {
        case Seq(text) => {
          Json.parse(text.asString) match {
            case JsArray(elems) => IntPrimitive(elems.length)
            case j => throw new RAException(s"Not an Array: $j")
          }
        }
      },
      (_) => TInt()
    )
  }

}