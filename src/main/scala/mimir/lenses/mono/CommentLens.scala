package mimir.lenses.mono

import java.sql.SQLException
import play.api.libs.json._
import sparsity.Name
import mimir.Database
import mimir.algebra._
import mimir.lenses._
import mimir.util.NameLookup

case class CommentLensConfig(
  targetColumn: Option[ID],
  message: Either[String, ID]
)
object CommentLensConfig
{
  implicit val writes = new Writes[CommentLensConfig] { def writes(config: CommentLensConfig) =
    JsObject(Map(
      "target" -> Json.toJson(config.targetColumn)
    ) ++ (
      config.message match { 
        case Left(msg) => Map("message" -> JsString(msg))
        case Right(col) => Map("column" -> Json.toJson(col))
      }
    ))
  }
  implicit val reads: Reads[CommentLensConfig] =
    JsPath.read[Map[String, String]].map { json => 
      CommentLensConfig(
        json.get("target").map { ID(_) },
        json.get("message").map { Left(_) }
          .getOrElse { 
            json.get("column").map { col => Right(ID(col)) }
                .getOrElse { throw new SQLException(s"Invalid Comment Lens Config: $json")} }
      )
    }
}

object CommentLens extends MonoLens
{

  def train(
    db: Database, 
    name: ID, 
    query: Operator,
    configJson: JsValue 
  ): JsValue =
  {
    // Use this opportunity to validate the config
    val config = configJson.as[CommentLensConfig]

    val columnLookup = LensUtils.columnLookupFunction(query)

    val newTarget = config.targetColumn.map { col => columnLookup(col.id) }

    val newMessage = 
      config.message match {
        case Left(message) => Left(message)
        case Right(col) => columnLookup(col.id)
      }

    Json.toJson(config)
  }

  def view(
    db: Database, 
    name: ID, 
    query: Operator, 
    configJson: JsValue, 
    friendlyName: String
  ): Operator = 
  {
    val config = configJson.as[CommentLensConfig]

    def applyWarning(e:Expression) =
      Caveat(
        name,
        e,
        Seq(RowIdVar()),
        config.message match {
          case Left(message) => StringPrimitive(message)
          case Right(col) => Var(col)
        }
      )

    config.targetColumn match {
      case None => 
        query.filter { applyWarning(BoolPrimitive(true)) }
      case Some(col) =>
        query.alterColumnsByID(
          col -> applyWarning(Var(col))
        )
    }
  }

}