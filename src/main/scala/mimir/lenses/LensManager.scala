package mimir.lenses;

import java.sql._;
import scala.collection.concurrent.TrieMap;
import scala.concurrent.Future;
import scala.concurrent.ExecutionContext.Implicits.global;

import mimir.Database
import mimir.algebra._
import mimir.ctables._
import mimir.sql._
import mimir.models._
import mimir.util.JDBCUtils
import mimir.util.ExperimentalOptions

class LensManager(db: Database) {
  val lensTypes = TrieMap[String,((Database,String,Operator,Seq[Expression]) => 
                              (Operator,TraversableOnce[Model]))](
    "MISSING_VALUE"     -> MissingValueLens.create _,
    "DOMAIN"            -> MissingValueLens.create _,
    "KEY_REPAIR"        -> RepairKeyLens.create _,
    "REPAIR_KEY"        -> RepairKeyLens.create _,
    "COMMENT"           -> CommentLens.create _,
    "MISSING_KEY"       -> MissingKeyLens.create _,
    "PICKER"            -> PickerLens.create _,
    "GEOCODE"           -> GeocodingLens.create _
  )

  def init(): Unit =
  {
    // no-op for now.
  }

  def create(
    t: String, 
    name: String, 
    query: Operator, 
    args: Seq[Expression]
  ): Unit =
  {
    val saneName = name.toUpperCase
    val constructor =
      lensTypes.get(t.toUpperCase) match {
        case Some(impl) => impl
        case None => throw new SQLException("Invalid Lens Type '"+t.toUpperCase+"'")
      }

    // Construct the appropriate lens
    val (view, models) = constructor(db, saneName, query, args)

    // Create a lens query
    db.views.create(saneName, view)

    // Persist the associated models
    for(model <- models){
      db.models.persist(model, s"LENS:$saneName")
    }
    
    val updateModels = Future
    {
      this.updateModels()
    }
    
  }

  def drop(name: String): Unit =
  {
    db.views.drop(name)
    db.models.dropOwner(s"LENS:$name")
  }
  
  def updateModels() : Unit = 
  {
    
  }

}
