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
import mimir.serialization.AlgebraJson._

class LensManager(db: Database) 
  extends ViewSchemaProvider
  with LazyLogging 
{

  val lensTypes = Map[ID,Lens](
    // ID("MISSING_VALUE")     -> MissingValueLens.create _,
    ID("DOMAIN")            -> DomainLens,
    // ID("KEY_REPAIR")        -> RepairKeyLens.create _,
    // ID("REPAIR_KEY")        -> RepairKeyLens.create _,
    ID("COMMENT")           -> CommentLens
    // ID("MISSING_KEY")       -> MissingKeyLens.create _,
    // ID("PICKER")            -> PickerLens.create _,
    // ID("GEOCODE")           -> GeocodingLens.create _
  )

  val monoLensTypes = 
    lensTypes.collect { case (t, _:MonoLens) => t }
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
    name: ID, 
    query: Operator, 
    config: JsValue,
    friendlyName: Option[String] = None,
    orReplace: Boolean = false
  )
  {
    logger.debug(s"Create Lens: $name ${friendlyName.map { "("+_+")"}.getOrElse("") }")

    if(lenses.exists(name) && !orReplace){
      throw new SQLException(s"Lens $name already exists")
    }

    val lens =
      lensTypes.get(t) match {
        case Some(impl) => impl
        case None => throw new SQLException("Invalid Lens Type '"+t+"'")
      }

    val trainedConfig = 
      lens.train(db, name, query, config)

    lenses.put(name, Seq(
      StringPrimitive(t.id),
      StringPrimitive(Json.toJson(query).toString),
      StringPrimitive(trainedConfig.toString),
      StringPrimitive(friendlyName.getOrElse(name.id))
    ))
  }

  private def lensForDetails(details: Seq[PrimitiveValue]): Lens =
    lensTypes(ID(details(0).asString))

  private def queryForDetails(details: Seq[PrimitiveValue]): Operator =
    Json.parse(details(1).asString).as[Operator]

  private def configForDetails(details: Seq[PrimitiveValue]): JsValue =
    Json.parse(details(2).asString)

  def retrain(name: ID)
  {
    val (_, details) = lenses.get(name)
                             .getOrElse { throw new SQLException(s"Invalid lens $name") }
    val retrainedConfig = 
      lensForDetails(details).train(
        db, 
        name, 
        queryForDetails(details), 
        configForDetails(details)
      )
    lenses.update(name, Map( 
      ID("ARGS") -> StringPrimitive(retrainedConfig.toString)
    ))
  }

  def drop(name: ID, ifExists: Boolean = false)
  {
    val details = 
      lenses.get(name) match {
        case Some((_, details)) => details
        case None if ifExists => return
        case None => throw new SQLException(s"Invalid lens $name")
      }

    lensForDetails(details) match {
      case cleanup:LensNeedsCleanup => 
        cleanup.drop(
          db, 
          name, 
          configForDetails(details)
        )
      case _ => ()
    }
    lenses.rm(name)
  }

  def listTables: Seq[ID] =
  {
    lenses.all
          .filter { case (name, detail) => monoLensTypes contains ID(detail(0).asString) }
          .map { _._1 }
  }

  def tableSchema(name: ID): Option[Seq[(ID, Type)]] =
  {
    val (_,details) = lenses.get(name).getOrElse { return None }
    lensForDetails(details) match {
      case monoLens:MonoLens => 
        Some(monoLens.schema(
          db, 
          name, 
          queryForDetails(details), 
          configForDetails(details),
          details(3).asString
        ))
    }
  }

  def view(name: ID): Operator =
  {
    val (_,details) = lenses.get(name).getOrElse { throw new SQLException(s"Invalid lens $name") }
    lensForDetails(details) match {
      case monoLens:MonoLens => 
        monoLens.view(
          db, 
          name, 
          queryForDetails(details), 
          configForDetails(details),
          details(3).asString
        )
    }    
  }
}

object LensManager
{
  val SCHEMA = ID("LENS")
}