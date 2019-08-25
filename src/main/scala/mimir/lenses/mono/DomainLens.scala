package mimir.lenses.mono

import java.sql.SQLException
import play.api.libs.json._
import sparsity.Name
import mimir.algebra._
import mimir.Database
import mimir.lenses._
import mimir.nullables.InvalidValueHandler
import mimir.parser.ExpressionParser
import mimir.serialization.AlgebraJson._
import mimir.util.NameLookup

case class DomainLensColumnConfig(
  constraint: Expression,
  mode: ID
)
object DomainLensColumnConfig
{
  implicit val format: Format[DomainLensColumnConfig] = Json.format
}


object DomainLens extends MonoLens
{
  def train(
    db: Database,
    name: ID,
    query: Operator,
    config: JsValue
  ): JsValue = 
  {
    val columnLookup = LensUtils.columnLookupFunction(query)

    val newConfig:Map[String, DomainLensColumnConfig] = 
      config.as[Map[String,Map[String, JsValue]]]
            .map { case (col, spec) =>
              val caseSensitiveCol = columnLookup(col)

              val constraint = spec.get("constraint") match {
                case Some(JsString(expr)) => ExpressionParser.apply(expr)
                case Some(j:JsObject) => j.as[Expression]
                case None | Some(JsNull) => Not(IsNullExpression(Var(caseSensitiveCol)))
                case other => throw new SQLException("Invalid constraint")
              }

              val mode = spec.get("mode") match {
                case Some(JsString(mode)) => ID(mode.toUpperCase)
                case None => InvalidValueHandler.DEFAULT
                case _ => throw new SQLException("Improperly formatted invalid handler mode: $mode")
              }

              InvalidValueHandler.rules.get(mode) match {
                case None => 
                  throw new SQLException(s"Invalid value repair mode: $mode (allowed modes include ${InvalidValueHandler.rules.keys.mkString(", ")}")
                case Some(handler) => 
                  handler.train(db, name, caseSensitiveCol, constraint, query)
              }

              caseSensitiveCol.id -> DomainLensColumnConfig(constraint, mode)
            }

    Json.toJson(newConfig)
  }
  
  def view(
    db: Database,
    name: ID,
    inputQuery: Operator,
    configJson: JsValue,
    friendlyName: String
  ): Operator = 
  {
    val config = configJson.as[Map[String, DomainLensColumnConfig]]

    config.foldLeft(inputQuery) { (query, colAndRepair) =>

      val handler = InvalidValueHandler.rules(colAndRepair._2.mode)
      handler.view(
        db, 
        name, 
        ID(colAndRepair._1),
        query, 
        colAndRepair._2.constraint,
        friendlyName
      )
    }
  }
}