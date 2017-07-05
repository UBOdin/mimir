package mimir.algebra.function;

import play.api.libs.json._
import mimir.algebra._
import mimir.serialization.Json
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
    Json.toPrimitive(t, extract(args))
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

  def register(fr: FunctionRegistry)
  {
    fr.register("JSON_EXTRACT", extractAny(_), (_) => TString())
    fr.register("JSON_EXTRACT_INT", extract(_, TInt()), (_) => TInt())
    fr.register("JSON_EXTRACT_FLOAT", extract(_, TFloat()), (_) => TFloat())
    fr.register("JSON_EXTRACT_STR", extract(_, TString()), (_) => TString())
    fr.register("JSON_ARRAY",
      (params: Seq[PrimitiveValue]) => 
        StringPrimitive(
          JsArray(
            params.map( Json.ofPrimitive(_) )
          ).toString
        ),
      (_) => TString()
    )
    fr.register("JSON_OBJECT", 
      (params: Seq[PrimitiveValue]) => {
        StringPrimitive(
          JsObject(
            params.grouped(2).map {
              case Seq(k, v) => (k.asString -> Json.ofPrimitive(v))
            }.toMap
          ).toString
        )
      },
      (_) => TString()
    )
    fr.register("JSON_ARRAY_LENGTH",
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