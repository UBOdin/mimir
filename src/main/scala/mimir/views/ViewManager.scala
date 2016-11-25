package mimir.views;

import mimir._;
import mimir.algebra._;

class ViewManager(db:Database) {
  
  val viewTable = "MIMIR_VIEWS"

  def init(): Unit = 
  {
    db.backend.update(s"""
      CREATE TABLE $viewTable(
        name varchar(30), 
        query text,
        PRIMARY KEY(name)
      )""")
  }

  def createView(name: String, query: Operator): Unit =
  {
    db.backend.update(s"INSERT INTO $viewTable(name, query) VALUES (?,?)", 
      List(
        StringPrimitive(name), 
        StringPrimitive(db.querySerializer.serialize(query))
      ))
  }

  def alterView(name: String, query: Operator): Unit =
  {
    db.backend.update(s"UPDATE $viewTable SET query=? WHERE name=?", 
      List(
        StringPrimitive(db.querySerializer.serialize(query)),
        StringPrimitive(name)
      )) 
  }

  def dropView(name: String): Unit =
  {
    db.backend.update(s"DELETE FROM $viewTable WHERE name=?", 
      List(StringPrimitive(name))
    )
  }

  def getView(name: String): Option[Operator] =
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

  def listViews(): List[String] =
  {
    db.backend.
      resultRows(s"SELECT name FROM $viewTable").
      flatten.
      map( _.asString ).
      toList
  }

}