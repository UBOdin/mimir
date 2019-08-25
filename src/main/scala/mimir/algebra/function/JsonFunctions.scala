package mimir.algebra.function;

import play.api.libs.json._
import mimir.algebra._
import mimir.serialization.AlgebraJson._
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
    castJsonToPrimitive(t, extract(args))
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
    fr.register(ID("json_extract"), extractAny(_), (_) => TString())
    fr.register(ID("json_extract_int"), extract(_, TInt()), (_) => TInt())
    fr.register(ID("json_extract_float"), extract(_, TFloat()), (_) => TFloat())
    fr.register(ID("json_extract_str"), extract(_, TString()), (_) => TString())
    fr.register(ID("json_array"),
      (params: Seq[PrimitiveValue]) => 
        StringPrimitive(
          JsArray(
            params.map( Json.toJson(_) )
          ).toString
        ),
      (_) => TString()
    )
    fr.register(ID("json_object"), 
      (params: Seq[PrimitiveValue]) => {
        StringPrimitive(
          JsObject(
            params.grouped(2).map {
              case Seq(k, v) => (k.asString -> Json.toJson(v))
            }.toMap
          ).toString
        )
      },
      (_) => TString()
    )
    fr.register(ID("json_array_length"),
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