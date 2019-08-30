package mimir.lenses.mono

import java.sql.SQLException
import play.api.libs.json._
import sparsity.Name
import mimir.Database
import mimir.algebra._
import mimir.lenses._
import mimir.util.NameLookup
import mimir.serialization.AlgebraJson._

case class CommentLensConfig(
  targetColumn: Option[ID],
  message: Expression,
  condition: Expression = BoolPrimitive(true)
)
object CommentLensConfig
{
  implicit val format:Format[CommentLensConfig] = Json.format
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

    val columnLookup = OperatorUtils.columnLookupFunction(query)

    Json.toJson(CommentLensConfig(
      config.targetColumn.map { col => columnLookup(Name(col.id)) },
      ExpressionUtils.rebindCaseInsensitive(config.message, columnLookup),
      ExpressionUtils.rebindCaseInsensitive(config.condition, columnLookup)
    ))
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
      config.condition
            .thenElse { 
                Caveat(
                  name,
                  e,
                  Seq(RowIdVar()),
                  config.message
                )
            } { e }

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