package mimir.algebra.function;

import java.util.Set

import com.github.wnameless.json.flattener.JsonFlattener
import mimir.algebra._
import mimir.serialization.Json
import mimir.sql.sqlite.JsonExplorerProject.createFullObjectSet
import mimir.util.JsonPlay.{AllData, ExplorerObject, ObjectTracker, TypeData}
import mimir.util._
import play.api.libs.json._

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

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

  def jsonClusterProject(args: Seq[PrimitiveValue]): PrimitiveValue =
  {
    if(args(0) != null){
      val jsonString: String = args(0).toString
      var jsonLeafKeySet: Set[String] = null

      try {
        val jsonMap: java.util.Map[String,AnyRef] = JsonFlattener.flattenAsMap(jsonString) // create a flat map of the json object
        jsonLeafKeySet = jsonMap.keySet()
      }
      catch{
        case e: Exception => {
          //              println(s"Not of JSON format in Json_Explorer_Project, so null returned: $jsonString")
          NullPrimitive()
        } // is not a proper json format so return null since there's nothing we can do about this right now
      } // end try catch

      StringPrimitive(jsonLeafKeySet.toString)
    }
    else{
      NullPrimitive()
    }
  }

  def jsonExplorerProject(args: Seq[PrimitiveValue]): PrimitiveValue =
  {

    val dataList: ListBuffer[AllData] = ListBuffer[AllData]()

    args match {
      case Seq(row) => {
        if(row.isInstanceOf[StringPrimitive]) { // the value is not null, so perform operations needed
          val jsonString: String = row.asString

          // try to clean up the json object, might want to replace this later
          var clean = jsonString.replace("\\\\", "") // clean the string of variables that will throw off parsing
          clean = clean.replace("\\\"", "")
          clean = clean.replace("\\n", "")
          clean = clean.replace("\\r", "")
          clean = clean.replace("\n", "")
          clean = clean.replace("\r", "")
          try {
            val jsonMap: java.util.Map[String,AnyRef] = JsonFlattener.flattenAsMap(clean) // create a flat map of the json object
            val jsonMapKeySet: Set[String] = jsonMap.keySet()
            val fullKeySet: ListBuffer[String] = createFullObjectSet(jsonMapKeySet)
            for (key: String <- fullKeySet){ // iterate through each key which can be thought of as a column
              jsonMapKeySet.contains(key) match {
                case true => // it is a leaf and is a type
                  val jsonType: String = Type.getType(jsonMap.get(key).toString).toString() // returns the
                  val tJson: TypeData = JsonPlay.TypeData(jsonType,1)
                  val dJson: AllData = JsonPlay.AllData(key,Option[Seq[TypeData]](Seq[TypeData]((tJson))),None)
                  dataList += dJson

                case false => // it is an object and has no type for this record
                  var objectSeq: ListBuffer[String] = ListBuffer[String]()
                  jsonMapKeySet.asScala.map((x: String) => {
                    val objectList = x.split("\\.")
                    if(!x.equals(key)) {
                      if(objectList.contains(key) && !objectList.last.equals(key)){ // contains key and it's not the last object
                        objectSeq += x
                      }
                    }
                  })
                  val dJson: AllData = JsonPlay.AllData(key,None,Option[Seq[ObjectTracker]](Seq[ObjectTracker](JsonPlay.ObjectTracker(objectSeq.toSeq,1))))
                  dataList += dJson
              }
            }
          }
          catch{
            case e: Exception => {
              println(s"Not of JSON format in Json_Explorer_Project, so null returned: $jsonString")
              return NullPrimitive()
            } // is not a proper json format so return null since there's nothing we can do about this right now
          }
          val jsonResult: ExplorerObject = ExplorerObject(dataList,1)
          val output: JsValue = play.api.libs.json.Json.toJson(jsonResult)
          return StringPrimitive(play.api.libs.json.Json.stringify(output))
        }
        else {
          return NullPrimitive()
        }
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
    fr.register("JSON_CLUSTER_PROJECT"   , jsonExplorerProject(_), (_) => TString())
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