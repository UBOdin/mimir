package mimir.lenses.mono

import java.sql.SQLException
import play.api.libs.json._
import sparsity.Name

import mimir.Database
import mimir.algebra._
import mimir.ctables.Reason
import mimir.lenses._

case class MergeColumnLensConfig(
  target: ID,
  sources: Seq[ID]
)

object MergeColumnLensConfig
{
  implicit val format:Format[MergeColumnLensConfig] = Json.format
}

object MergeColumnLens extends MonoLens
{
  def train(
    db: Database,
    name: ID,
    query: Operator,
    config: JsValue
  ): JsValue = 
  {
    val columnLookup = OperatorUtils.columnLookupFunction(query)
    Json.toJson(
      config match {
        case o:JsObject => o.as[MergeColumnLensConfig]
        case a:JsArray => {
          val targets = a.as[Seq[String]]
                         .map { col => columnLookup(Name(col)) }
          MergeColumnLensConfig(
            targets.head,
            targets
          )
        }
        case _ => throw new SQLException(s"Invalid lens configuration $config")
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
    val config = configJson.as[MergeColumnLensConfig]
    val isSource = config.sources.toSet
    val normalColumns = query.columnNames.filter { !isSource(_) }

    val firstSource = config.sources.head
    val areEqualCondition = 
      ExpressionUtils.makeAnd(
        config.sources.tail.map { _.eq(firstSource) }
      )
    val message =
      Function(ID("concat"), Seq(
        StringPrimitive(s"When merging $friendlyName.${config.target}, expected "),
        Var(firstSource).as(TString()),
        StringPrimitive(" to be equal to ")
      ) ++ 
        config.sources.tail
          .flatMap { col => Seq(StringPrimitive(", "), Var(col)) }
          .tail
      )

    query.mapByID( (
      normalColumns.map { col => col -> Var(col) }
      :+ ( config.target -> 
              areEqualCondition.thenElse 
                { Var(firstSource) }
                { Caveat(name, Var(firstSource), Seq(RowIdVar()), message)}

      )
    ):_* )
  }

  def warnings(
    db: Database, 
    name: ID, 
    query: Operator, 
    cols: Seq[ID],
    configJson: JsValue, 
    friendlyName: String
  ) = Seq[Reason]()
}
