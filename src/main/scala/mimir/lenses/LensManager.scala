package mimir.lenses

import java.sql._
import play.api.libs.json._
import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir.Database
import mimir.algebra._
import mimir.ctables._
import mimir.sql._
import mimir.metadata._
import mimir.models._
import mimir.data.ViewSchemaProvider
import mimir.util.JDBCUtils
import mimir.util.ExperimentalOptions
import mimir.lenses.mono._
// import mimir.lenses.multi._
import mimir.serialization.AlgebraJson._

class LensManager(db: Database) 
  extends ViewSchemaProvider
  with LazyLogging 
{

  val lensTypes = Map[ID,Lens](
    ID("TYPE_INFERENCE")    -> CastColumnsLens,
    ID("CAST")              -> CastColumnsLens,
    ID("MISSING_VALUE")     -> DomainLens,
    ID("DOMAIN")            -> DomainLens,
    ID("KEY_REPAIR")        -> RepairKeyLens,
    ID("REPAIR_KEY")        -> RepairKeyLens,
    ID("COMMENT")           -> CommentLens,
    ID("MISSING_KEY")       -> MissingKeyLens,
    ID("PICKER")            -> MergeColumnLens,
    ID("MERGE_COLUMNS")     -> MergeColumnLens,
    ID("GEOCODE")           -> GeocodingLens
  )

  val monoLensTypes = 
    lensTypes.collect { case (t, _:MonoLens) => t }
             .toSet

  val multiLensTypes = 
    lensTypes.collect { case (t, _:MultiLens) => t }
             .toSet


  val lenses = db.metadata.registerMap(
    ID("MIMIR_LENSES"), Seq(
      InitMap(Seq(
        ID("TYPE")          -> TString(),
        ID("QUERY")         -> TString(),
        ID("ARGS")          -> TString(),
        ID("FRIENDLY_NAME") -> TString()
      ))
    ))

  def create(
    t: ID, 
    lensName: ID, 
    query: Operator, 
    config: JsValue,
    friendlyName: Option[String] = None,
    orReplace: Boolean = false
  )
  {
    logger.debug(s"Create Lens: $lensName ${friendlyName.map { "("+_+")"}.getOrElse("") }")

    if(lenses.exists(lensName) && !orReplace){
      throw new SQLException(s"Lens $lensName already exists")
    }

    val lens =
      lensTypes.get(t) match {
        case Some(impl) => impl
        case None => throw new SQLException("Invalid Lens Type '"+t+"'")
      }

    val trainedConfig = 
      lens.train(db, lensName, query, config)

    lenses.put(lensName, Seq(
      StringPrimitive(t.id),
      StringPrimitive(Json.toJson(query).toString),
      StringPrimitive(trainedConfig.toString),
      StringPrimitive(friendlyName.getOrElse(lensName.id))
    ))
  }

  private def lensForDetails(details: Seq[PrimitiveValue]): Lens =
    lensTypes(ID(details(0).asString))

  private def queryForDetails(details: Seq[PrimitiveValue]): Operator =
    Json.parse(details(1).asString).as[Operator]

  private def configForDetails(details: Seq[PrimitiveValue]): JsValue =
    Json.parse(details(2).asString)

  def retrain(lens: ID)
  {
    val (_, details) = lenses.get(lens)
                             .getOrElse { throw new SQLException(s"Invalid lens $lens") }
    val retrainedConfig = 
      lensForDetails(details).train(
        db, 
        lens, 
        queryForDetails(details), 
        configForDetails(details)
      )
    lenses.update(lens, Map( 
      ID("ARGS") -> StringPrimitive(retrainedConfig.toString)
    ))
  }

  def drop(lens: ID, ifExists: Boolean = false)
  {
    val details = 
      lenses.get(lens) match {
        case Some((_, details)) => details
        case None if ifExists => return
        case None => throw new SQLException(s"Invalid lens $lens")
      }

    lensForDetails(details) match {
      case cleanup:LensNeedsCleanup => 
        cleanup.drop(
          db, 
          lens, 
          configForDetails(details)
        )
      case _ => ()
    }
    lenses.rm(lens)
  }

  def acknowledge(lens: ID, key: Seq[PrimitiveValue])
  {
    ???
  }

  def acknowledgeAll(lens: ID)
  {
    ???
  }

  def isAcknowledged(lens: ID, key: Seq[PrimitiveValue]): Boolean =
  {
    ???
  }

  def areAllAcknowledged(lens: ID): Boolean =
  {
    ???
  }

  def acknowledgedKeys(lens: ID): Seq[Seq[PrimitiveValue]] = 
  {
    ???
  }

  def get(lens: ID): (Lens, Operator, JsValue) =
  {
    val (_, details) = lenses.get(lens)
                             .getOrElse { throw new SQLException(s"Invalid lens $lens") }
    (
      lensForDetails(details), 
      queryForDetails(details),
      configForDetails(details)
    )
  }


  def listTables: Seq[ID] =
  {
    lenses.all
          .filter { case (lens, detail) => monoLensTypes contains ID(detail(0).asString) }
          .map { _._1 }
  }

  def tableSchema(lens: ID): Option[Seq[(ID, Type)]] =
  {
    val (_,details) = lenses.get(lens).getOrElse { return None }
    lensForDetails(details) match {
      case monoLens:MonoLens => 
        Some(monoLens.schema(
          db, 
          lens, 
          queryForDetails(details), 
          configForDetails(details),
          details(3).asString
        ))
      case _:MultiLens => return None
    }
  }

  def view(lens: ID): Operator =
  {
    val (_,details) = lenses.get(lens).getOrElse { throw new SQLException(s"Invalid lens $lens") }
    lensForDetails(details) match {
      case monoLens:MonoLens => 
        LensView(
          None,
          lens,
          monoLens.view(
            db, 
            lens, 
            queryForDetails(details), 
            configForDetails(details),
            details(3).asString
          )
        )
      case multiLens:MultiLens =>
        throw new SQLException(s"Invalid lens $lens")
    }    
  }

  private def allMultiLenses: Seq[(ID, Seq[PrimitiveValue])] =
  {
    lenses.all
          .filter { case (lens, details) => multiLensTypes contains ID(details(0).asString) }
  }
  def schemaProviderFor(lens: ID): Option[MultiLensSchemaProvider] =
    lenses.get(lens).map { case (_, details) => assembleSchemaProvider(lens, details) }

  private def assembleSchemaProvider(
    lens: ID, 
    details: Seq[PrimitiveValue]
  ): MultiLensSchemaProvider =
    MultiLensSchemaProvider(
      lensForDetails(details).asInstanceOf[MultiLens],
      db,
      lens, 
      queryForDetails(details),
      configForDetails(details),
      details(3).asString
    )
  
  def allSchemaProviders: Seq[(ID, MultiLensSchemaProvider)] =
    allMultiLenses.map { case (lens, details) => 
      lens -> assembleSchemaProvider(lens, details)
    }

}

object LensManager
{
  val SCHEMA = ID("LENS")
}