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
    val columnLookup = OperatorUtils.columnLookupFunction(query)

    val newConfig:Map[String, DomainLensColumnConfig] = 
      // Interpret a variety of different ways of formulating the lens configuration
      //   - Object -> 1 field per column
      //   - Array -> An array of string representing column names
      //   - String -> A single column name to parse
      //   - Null -> All columns
      // the result is a map with a single element per column
      (config match {
        case JsObject(v) => v
        case JsArray(elems) => elems.map { _.as[String] -> JsNull }
                                    .toMap
        case JsNull => query.columnNames
                            .map { _.id -> JsNull }
                            .toMap
        case JsString(s) => Map( s -> JsNull )
        case _ => throw new SQLException("Invalid lens configuration $config")
      })
      // For each column, interpret the resulting value.  There's a map of key/value configuration
      // options that we expect.  Interpret alternatives as follows:
      //   - Null -> Use default configuration
      //   - Object -> Use as provided
        .map { 
          case (col, JsNull) => col -> Map[String, JsValue]()
          case (col, JsObject(v)) => col -> v
          case (col, other) => throw new SQLException("Invalid lens configuration for column $col: $other")
        }
      // Actually process the configuration
        .map { case (col, spec) =>

          // Resolve column names case-insensitively
          val caseSensitiveCol = columnLookup(Name(col))

          // Resolve the column constraint
          val constraint = spec.get("constraint") match {
            case Some(JsString(expr)) => ExpressionParser.apply(expr)
            case Some(j:JsObject) => j.as[Expression]
            case None | Some(JsNull) => Not(IsNullExpression(Var(caseSensitiveCol)))
            case other => throw new SQLException("Invalid constraint")
          }

          // Resolve the resolution mode
          val mode = spec.get("mode") match {
            case Some(JsString(mode)) => ID(mode.toUpperCase)
            case None => InvalidValueHandler.DEFAULT
            case _ => throw new SQLException("Improperly formatted invalid handler mode: $mode")
          }

          // Perform any mode-specific training
          InvalidValueHandler.rules.get(mode) match {
            case None => 
              throw new SQLException(s"Invalid value repair mode: $mode (allowed modes include ${InvalidValueHandler.rules.keys.mkString(", ")}")
            case Some(handler) => 
              handler.train(db, name, caseSensitiveCol, constraint, query)
          }

          caseSensitiveCol.id -> DomainLensColumnConfig(constraint, mode)
        }
        .toMap

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