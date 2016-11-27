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
    "MISSING_VALUE" -> MissingValueLens.create _
  )

  def init(): Unit =
  {

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

    constructor(db, name, query, args)
  }

  def dropLens(name: String): Unit =
  {

  }

}