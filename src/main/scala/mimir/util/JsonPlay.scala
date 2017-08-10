package mimir.util

import java.util.Set

import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.functional.syntax._

object JsonPlay {

  // Types
  case class ObjectTracker(objectRelationship: Seq[String], objectCount: Int)
  case class TypeData(typeName: String, typeCount: Int)
  case class AllData(name: String, td: Option[Seq[TypeData]], ot: Option[Seq[ObjectTracker]])
  case class ExplorerObject(data: Seq[AllData], rowCount: Int)

  // Writers
  implicit val objectTrackerWrites = new Writes[ObjectTracker] {
    def writes(ot: ObjectTracker) = Json.obj(
      "objectRelationship" -> ot.objectRelationship,
      "objectCount" -> ot.objectCount
    )
  }

  implicit val typeDataWrites = new Writes[TypeData] {
    def writes(td: TypeData) = Json.obj(
      "typeName" -> td.typeName,
      "typeCount" -> td.typeCount
    )
  }

  implicit val allDataWrites = new Writes[AllData] {
    def writes(ad: AllData) = Json.obj(
      "path" -> ad.name,
      "typeData" -> ad.td,
      "objectData" -> ad.ot
    )
  }

  implicit val explorerObjectWrites = new Writes[ExplorerObject] {
    def writes(eo: ExplorerObject) = Json.obj(
      "data" -> eo.data,
      "rowCount" -> eo.rowCount
    )
  }

  // Readers
  implicit val objectTrackerReads: Reads[ObjectTracker] = (
    (JsPath \ "objectRelationship").read[Seq[String]] and
      (JsPath \ "objectCount").read[Int]
    )(ObjectTracker.apply _)

  implicit val typeDataReads: Reads[TypeData] = (
    (JsPath \ "typeName").read[String] and
      (JsPath \ "typeCount").read[Int]
    )(TypeData.apply _)

  implicit val allDataReads: Reads[AllData] = (
    (JsPath \ "path").read[String] and
      (JsPath \ "typeData").readNullable[Seq[TypeData]] and
        (JsPath \ "objectData").readNullable[Seq[ObjectTracker]]
    )(AllData.apply _)

  implicit val explorerObjectReads: Reads[ExplorerObject] = (
    (JsPath \ "data").read[Seq[AllData]] and
      (JsPath \ "rowCount").read[Int]
    )(ExplorerObject.apply _)

}
