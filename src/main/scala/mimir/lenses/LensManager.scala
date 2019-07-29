package mimir.lenses

import java.sql._

import mimir.Database
import mimir.algebra._
import mimir.ctables._
import mimir.sql._
import mimir.models._
import mimir.util.JDBCUtils
import mimir.util.ExperimentalOptions
import com.typesafe.scalalogging.slf4j.LazyLogging

class LensManager(db: Database) extends LazyLogging {

  val lensTypes = Map[ID,((Database,ID,String,Operator,Seq[Expression]) => 
                              (Operator,TraversableOnce[Model]))](
    ID("MISSING_VALUE")     -> MissingValueLens.create _,
    ID("DOMAIN")            -> MissingValueLens.create _,
    ID("KEY_REPAIR")        -> RepairKeyLens.create _,
    ID("REPAIR_KEY")        -> RepairKeyLens.create _,
    ID("COMMENT")           -> CommentLens.create _,
    ID("MISSING_KEY")       -> MissingKeyLens.create _,
    ID("PICKER")            -> PickerLens.create _,
    ID("GEOCODE")           -> GeocodingLens.create _
  )

  def init(): Unit =
  {
    // no-op for now.
  }

  def create(
    t: ID, 
    name: ID, 
    query: Operator, 
    args: Seq[Expression],
    humanReadableName: Option[String] = None
  ): Unit =
  {
    logger.debug(s"Create Lens: $name ($humanReadableName)")
    val constructor =
      lensTypes.get(t) match {
        case Some(impl) => impl
        case None => throw new SQLException("Invalid Lens Type '"+t+"'")
      }

    // Construct the appropriate lens
    val (view, models) = constructor(db, name, humanReadableName.getOrElse(name.id), query, args)

    // Create a lens query
    db.views.create(name, view)

    // Persist the associated models
    for(model <- models){
      db.models.persist(model, ID("LENS:",name))
    }
  }

  def drop(name: ID, ifExists: Boolean = false): Unit =
  {
    db.views.drop(name, ifExists)
    db.models.dropOwner(ID("LENS:",name))
  }
}
