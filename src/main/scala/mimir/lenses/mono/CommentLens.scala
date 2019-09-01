package mimir.lenses.mono

import java.sql.SQLException
import play.api.libs.json._
import sparsity.Name
import mimir.Database
import mimir.algebra._
import mimir.lenses._
import mimir.util.NameLookup
import mimir.serialization.AlgebraJson._
import mimir.parser.ExpressionParser

case class CommentLensConfig(
  target: Option[ID],
  message: Expression,
  condition: Option[Expression] = None
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
    val columnLookup = OperatorUtils.columnLookupFunction(query)
    // Use this opportunity to validate the config

    Json.toJson(
      configJson match {
        case JsString(s) => CommentLensConfig(None, StringPrimitive(s), None)
        case JsObject(obj) => 
          CommentLensConfig(
            obj.get("target") match {
              case None | Some(JsNull) => None
              case Some(JsString(s)) => Some(columnLookup(Name(s)))
              case _ => throw new SQLException(s"Invalid lens configuration $configJson")
            },
            obj.get("message") match {
              case Some(JsString(s)) => StringPrimitive(s)
              case Some(o:JsObject) => o.as[Expression]
              case _ => throw new SQLException(s"Invalid lens configuration $configJson")
            },
            obj.get("condition") match {
              case None | Some(JsNull) => None
              case Some(JsString(s)) => Some(ExpressionParser.expr(s))
              case Some(o:JsObject) => Some(o.as[Expression])
              case _ => throw new SQLException(s"Invalid lens configuration $configJson")
            }
          )
        case _ => throw new SQLException(s"Invalid lens configuration $configJson")
      }
    )
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
            .getOrElse { BoolPrimitive(true) }
            .thenElse { 
                Caveat(
                  name,
                  e,
                  Seq(RowIdVar()),
                  config.message
                )
            } { e }

    config.target match {
      case None => 
        query.filter { applyWarning(BoolPrimitive(true)) }
      case Some(col) =>
        query.alterColumnsByID(
          col -> applyWarning(Var(col))
        )
    }
  }

}