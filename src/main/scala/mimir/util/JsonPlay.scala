package mimir.util

import java.util.Set

import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.functional.syntax._

object JsonPlay {

  case class TypeData(typeName: String, typeCount: Int)
  case class AllData(name: String, td: Seq[TypeData])
  case class ExplorerObject(data: Seq[AllData], rowCount: Int)

  implicit val typeDataWrites = new Writes[TypeData] {
    def writes(td: TypeData) = Json.obj(
      "typeName" -> td.typeName,
      "typeCount" -> td.typeCount
    )
  }

  implicit val allDataWrites = new Writes[AllData] {
    def writes(ad: AllData) = Json.obj(
      "path" -> ad.name,
      "typeData" -> ad.td
    )
  }

  implicit val explorerObjectWrites = new Writes[ExplorerObject] {
    def writes(eo: ExplorerObject) = Json.obj(
      "data" -> eo.data,
      "rowCount" -> eo.rowCount
    )
  }

  implicit val typeDataReads: Reads[TypeData] = (
    (JsPath \ "typeName").read[String] and
      (JsPath \ "typeCount").read[Int]
    )(TypeData.apply _)

  implicit val allDataReads: Reads[AllData] = (
    (JsPath \ "path").read[String] and
      (JsPath \ "typeData").read[Seq[TypeData]]
    )(AllData.apply _)

  implicit val explorerObjectReads: Reads[ExplorerObject] = (
    (JsPath \ "data").read[Seq[AllData]] and
      (JsPath \ "rowCount").read[Int]
    )(ExplorerObject.apply _)

}
