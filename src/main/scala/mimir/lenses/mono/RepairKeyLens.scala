package mimir.lenses.mono

import java.sql.SQLException
import play.api.libs.json._
import sparsity.Name
import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir.Database
import mimir.algebra._
import mimir.ctables.Reason
import mimir.exec.mode.UnannotatedBestGuess
import mimir.lenses._
import mimir.serialization.AlgebraJson._

case class RepairKeyLensConfig(
  key: Seq[ID],
  weight: Option[Expression]
)

object RepairKeyLensConfig
{
  implicit val format: Format[RepairKeyLensConfig] = Json.format
}

object RepairKeyLens 
  extends MonoLens
  with LazyLogging
{
  val SAMPLE_SIZE = 2000
  val DUPPED_VALUE_THRESHOLD = 0.2
  val TOTAL_ROWS = ID("TOTAL_ROWS")
  val COL_UNIQUE = ID("UNIQUE_", _:ID)
  val COL_ALTERNATIVES = ID("ALTERNATIVES_", _:ID)
  val CANDIDATE_KEY_TYPES = Set[Type](
    TString(),
    TDate(),
    TTimestamp(),
    TRowId()
  )

  def train(
    db: Database,
    name: ID,
    query: Operator,
    config: JsValue
  ): JsValue = 
  {
    val columnLookup = OperatorUtils.columnLookupFunction(query)
    val configCandidate:RepairKeyLensConfig = 
      config match {
        case JsNull => RepairKeyLensConfig(detectKey(db, query).toSeq, None)
        case JsObject(_) => config.as[RepairKeyLensConfig]
        case JsString(s) => RepairKeyLensConfig(Seq(columnLookup(Name(s))), None)
        case JsArray(keys) => RepairKeyLensConfig(
            keys.map { k => columnLookup(Name(k.as[String])) },
            None
          )
        case _ => throw new SQLException(s"Invalid lens configuration $config")
      }

    Json.toJson(configCandidate)
  }

  def detectKey(
    db: Database,
    query: Operator
  ): Option[ID] =
  {
    val candidateColumns = 
      db.typechecker
        .schemaOf(query)
        .filter { 
          case (col, t) => CANDIDATE_KEY_TYPES contains t
        }
        .map { _._1 }


    if(candidateColumns.isEmpty){
      logger.debug("No reasonable key columns available, defaulting to ROWID")
      return None
    }

    db.query(
      query
        .limit(SAMPLE_SIZE)
        .aggregate(
          (
            AggFunction(
              ID("count"),
              false,
              Seq(),
              TOTAL_ROWS
            ) +:
            candidateColumns.map { col =>
              AggFunction(
                ID("count"),
                true,
                Seq(),
                COL_UNIQUE(col)
              )
            }
          ):_*
        ),
      UnannotatedBestGuess
    ) { results =>
      if(!results.hasNext){
        throw new SQLException("No data, can't guess key column")
      }
      val row = results.next()
      val bestColumn =
        candidateColumns
             .maxBy { col => row(COL_UNIQUE(col)).asLong }
      val duppedValues = 
        1.0 - row(COL_UNIQUE(bestColumn)).asDouble / row(TOTAL_ROWS).asDouble
      if(duppedValues > DUPPED_VALUE_THRESHOLD){
        return None
      }
      return Some(bestColumn)
    }
  }

  def view(
    db: Database,
    name: ID,
    query: Operator,
    jsonConfig: JsValue,
    friendlyName: String
  ): Operator = 
  {
    val config = jsonConfig.as[RepairKeyLensConfig]

    // Shortcut for a no-op repair key lens
    if(config.key.isEmpty) { return query }

    if(config.weight != None){ ??? }

    val isKeyCol = config.key.toSet
    val nonKeyCols = query.columnNames.filter { !isKeyCol(_) }

    query
      .renameByID(
        config.key.map { k => k -> COL_UNIQUE(k) }:_*
      )
      .groupBy(config.key.map { k => Var(COL_UNIQUE(k)) }:_*)(
          nonKeyCols.flatMap { col => 
            Seq(
              AggFunction(
                ID("first"),
                false,
                Seq(Var(col)),
                COL_UNIQUE(col)
              ),
              AggFunction(
                ID("json_group_array"),
                true,
                Seq(CastExpression(Var(col), TString())),
                COL_ALTERNATIVES(col)
              )
            )
          }:_*
        )
      .mapByID(query.columnNames.map { 
        case col if isKeyCol(col) => col -> Var(COL_UNIQUE(col))
        case col => {
          val keyTuple: Seq[Expression] = 
            config.key.flatMap { k =>
              Seq(
                StringPrimitive(", "),
                StringPrimitive(s"$k: "),
                CastExpression(Var(COL_UNIQUE(k)), TString())
              )
            }.tail // strip off the leading ','
          val message = Function(ID("concat"), Seq[Expression](
              StringPrimitive(s"Non-unique key:")
            )++keyTuple++Seq[Expression](
              StringPrimitive(s"; Alternatives for $friendlyName.$col include "),
              Var(COL_ALTERNATIVES(col))
          ))
          val caveat = Caveat(
            name, 
            Var(COL_UNIQUE(col)),
            config.key.map { k => Var(COL_UNIQUE(k)) },
            message
          )

          col -> 
            Function(ID("json_array_length"), Seq(Var(COL_ALTERNATIVES(col))))
              .lte(1)
              .thenElse { Var(COL_UNIQUE(col)) } { caveat }
        }
      }:_*)
  
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
