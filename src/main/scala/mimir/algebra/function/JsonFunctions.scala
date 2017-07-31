package mimir.algebra.function;

import java.util.Set

import com.github.wnameless.json.flattener.JsonFlattener
import play.api.libs.json._
import mimir.algebra._
import mimir.serialization.Json
import mimir.util._
import scala.collection.JavaConverters._

import scala.util.parsing.json.JSONObject

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

  def jsonExplorerProject(args: Seq[PrimitiveValue]): PrimitiveValue =
  {

    var columnData: Map[String,Any] = Map[String,Any]()

    args match {
      case Seq(row) => {
        if(row.isInstanceOf[StringPrimitive]) { // the value is not null, so perform operations needed
          val jsonString:String = row.toString
          var clean = jsonString.replace("\\\\", "") // clean the string of variables that will throw off parsing
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

          try {
            val jsonMap: java.util.Map[String,AnyRef] = JsonFlattener.flattenAsMap(clean) // map of all the
            val jsonMapKeySet: Set[String] = jsonMap.keySet()
            for (key:String <- jsonMapKeySet.asScala){ // iterate through each key, for each key find it's type
              val jsonType:String = Type.getType(jsonMap.get(key).toString).toString()
              var typeData: Map[String,Integer] = Map[String,Integer]() // contains column type data, this is
              typeData = typeData + (jsonType -> 1)
              columnData = columnData + (key -> JSONObject(typeData).toString())
            }
          }
          catch{
            case e: Exception => throw new Exception("Json_Miner expects Json string as an input, check the input")
          }
        }
        // now return the json output
        // println(JSONObject(columnData).toString())

        return StringPrimitive(JSONObject(columnData).toString())
      }
      case _ => throw new Exception("JSON_Miner args are not of the right form") // should be single column of json text
    }

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
    fr.register("JSON_EXPLORER_PROJECT"   , jsonExplorerProject(_), (_) => TString())
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