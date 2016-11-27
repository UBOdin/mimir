package mimir.lenses;

import java.sql._

import mimir.Database
import mimir.algebra._
import mimir.ctables._
import mimir.sql._
import mimir.models._
import mimir.util.JDBCUtils

class LensManager(db: Database) {

  val lensTypes = Map[String,((Database,String,Operator,List[Expression]) => 
                              (Operator,List[Model]))](
    "MISSING_VALUE"  -> MissingValueLens.create _,
    "SCHEMA_MATCH"   -> SchemaMatchingLens.create _,
    "TYPE_INFERENCE" -> TypeInferenceLens.create _
  )

  def init(): Unit =
  {
    // no-op for now.
  }

  def createLens(
    t: String, 
    name: String, 
    query: Operator, 
    args: List[Expression]
  ): Unit =
  {
    val constructor =
      lensTypes.get(t) match {
        case Some(impl) => impl
        case None => throw new SQLException("Invalid Lens Type '"+t+"'")
      }

    val (view, models) = constructor(db, name, query, args)
    db.views.createView(name, view)
    models.foreach( model => 
      db.models.persistModel(model, s"LENS:$name")
    )
  }

  def dropLens(name: String): Unit =
  {
    db.views.dropView(name)
    db.models.dropOwner(s"LENS:$name")
  }

}

