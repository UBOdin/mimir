package mimir.views;

import mimir._;
import mimir.algebra._;
import com.typesafe.scalalogging.slf4j.LazyLogging

class ViewManager(db:Database) extends LazyLogging {
  
  val viewTable = "MIMIR_VIEWS"

  def init(): Unit = 
  {
    if(db.backend.getTableSchema(viewTable).isEmpty){
      db.backend.update(s"""
        CREATE TABLE $viewTable(
          NAME varchar(100), 
          QUERY text,
          PRIMARY KEY(name)
        )""")
    }
  }

  def create(name: String, query: Operator): Unit =
  {
    logger.debug(s"CREATE VIEW $name AS $query")
    db.backend.update(s"INSERT INTO $viewTable(name, query) VALUES (?,?)", 
      List(
        StringPrimitive(name), 
        StringPrimitive(db.querySerializer.serialize(query))
      ))
  }

  def alter(name: String, query: Operator): Unit =
  {
    db.backend.update(s"UPDATE $viewTable SET query=? WHERE name=?", 
      List(
        StringPrimitive(db.querySerializer.serialize(query)),
        StringPrimitive(name)
      )) 
  }

  def drop(name: String): Unit =
  {
    db.backend.update(s"DELETE FROM $viewTable WHERE name=?", 
      List(StringPrimitive(name))
    )
  }

  def get(name: String): Option[Operator] =
  {
    val results = 
      db.backend.resultRows(s"SELECT query FROM $viewTable WHERE name = ?", 
        List(StringPrimitive(name.toUpperCase))
      )
    results.take(1).flatten.toList.headOption.map( 
      { 
        case StringPrimitive(s) => 
          db.querySerializer.deserializeQuery(s)
      }
    )
  }

  def list(): List[String] =
  {
    db.backend.
      resultRows(s"SELECT name FROM $viewTable").
      flatten.
      map( _.asString ).
      toList
  }

  def listViewsQuery: Operator = 
  {
    Project(
      Seq(
        ProjectArg("TABLE_NAME", Var("NAME"))
      ),
      db.getTableOperator(viewTable)
    )
  }
  def listAttrsQuery: Operator = 
  {
    logger.warn("Constructing lens attribute list not implemented yet")
    EmptyTable(Seq(
      ("TABLE_NAME", TString()), 
      ("ATTR_NAME", TString()),
      ("ATTR_TYPE", TString()),
      ("IS_KEY", TBool())
    ))
  }

}