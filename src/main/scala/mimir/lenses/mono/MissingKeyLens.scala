package mimir.lenses.mono

import java.sql.SQLException
import play.api.libs.json._
import sparsity.Name
import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir.Database
import mimir.algebra._
import mimir.ctables.Reason
import mimir.lenses._
import mimir.serialization.AlgebraJson._
import mimir.statistics.{ DetectSeries, ColumnStepStatistics }

case class MissingKeyLensConfig(
  key: ID,
  t: Type,
  low: PrimitiveValue,
  high: PrimitiveValue,
  step: PrimitiveValue
)

object MissingKeyLensConfig
{
  implicit val format:Format[MissingKeyLensConfig] = Json.format
}

object MissingKeyLens 
  extends MonoLens
  with LazyLogging
{
  def train(
    db: Database,
    name: ID,
    query: Operator,
    configJson: JsValue
  ): JsValue = 
  {
    Json.toJson(
      configJson match {
        case JsNull => discoverKey(db, query)
        case JsObject(elems) => 
          elems.get("key") match {
            case None | Some(JsNull) => discoverKey(db, query)
            case Some(JsString(key)) => 
              if(    (elems contains "low")
                  && (elems contains "high")
                  && (elems contains "step") ){
                val keyType = db.typechecker.typeOf(Var(key), query)
                val stepType = Typechecker.escalate(keyType, keyType, Arith.Sub).get
                logger.debug(s"Building config for $key with type $keyType and step $stepType")
                logger.debug(s"... low = ${elems.get("low")}; high = ${elems.get("high")}; step = ${elems.get("step")}")
                MissingKeyLensConfig(
                  ID(key),
                  keyType,
                  Cast(keyType, elems.get("low").get.as[PrimitiveValue]),
                  Cast(keyType, elems.get("high").get.as[PrimitiveValue]),
                  Cast(stepType, elems.get("step").get.as[PrimitiveValue])
                )
              } else { trainOnKey(db, query, ID(key)) }
            case _ => throw new SQLException(s"Invalid configuration $configJson")
          }
        case JsString(key) => {
          val columnLookup = OperatorUtils.columnLookupFunction(query)
          trainOnKey(db, query, columnLookup(Name(key)))
        }
        case _ => throw new SQLException(s"Invalid configuration $configJson")
      }
    )
  }

  def discoverKey(
    db: Database,
    query: Operator
  ): MissingKeyLensConfig = 
  {
    val candidates = DetectSeries.seriesOf(db, query)
    if(candidates.isEmpty) { throw new SQLException("No valid key column") }
    val best = candidates.minBy { _.relativeStepStddev }
    makeConfig(
      best,
      db.typechecker.typeOf(Var(best.name), query)
    )
  }

  def trainOnKey(
    db: Database,
    query: Operator,
    key: ID
  ): MissingKeyLensConfig = 
  {
    makeConfig(
      DetectSeries.gatherStatistics(db, query, key), 
      db.typechecker.typeOf(Var(key), query)
    )
  }

  def makeConfig(stats: ColumnStepStatistics, t: Type): MissingKeyLensConfig =
  {
    val step = 
      if(stats.minStep.asDouble > 0) { stats.minStep }
      else { stats.meanStep }

    MissingKeyLensConfig(
      stats.name,
      stats.t,
      stats.low,
      stats.high,
      Cast(t, step)
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
    logger.debug(s"Creating view: $configJson")

    val config = configJson.as[MissingKeyLensConfig]

    logger.debug(s"Creating view: $config")

    val series = 
      HardTable(
        Seq(ID("_MIMIR_MISSING_KEY") -> config.t),
        DetectSeries.makeSeries(config.low, config.high, config.step)
                    .map { Seq(_) }
      )
    LeftOuterJoin(
      series,
      query.filter { Not(Var(config.key).isNull) },
      Var("_MIMIR_MISSING_KEY").eq( Var(config.key) )
    ).filter { 
        Var(config.key)
          .isNull
          .thenElse {
            Caveat(
              name,
              BoolPrimitive(true),
              Seq(Var("_MIMIR_MISSING_KEY")),
              Function(ID("concat"),Seq(
                StringPrimitive("Injected missing key: "),
                Var("_MIMIR_MISSING_KEY").as(TString())
              ))
            )
          } { BoolPrimitive(true) }
      }
     .removeColumnsByID(config.key)
     .renameByID(ID("_MIMIR_MISSING_KEY") -> config.key)
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
