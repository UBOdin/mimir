package mimir.views;

import mimir._;
import mimir.algebra.{Operator, Serialization, RAException}

class ViewManager(db:Database) {
  
  val viewTable = "MIMIR_VIEWS"

  def init(): Unit = 
  {
    db.backend.update(s"""
      CREATE TABLE $viewTable(
        name varchar(30), 
        query blob,
        PRIMARY KEY(name)
      )""")
  }

  def createView(name: String, query: Operator): Unit =
  {
    db.backend.update(s"INSERT INTO $viewTable(name, query) VALUES (?,?)", 
      List(
        StringPrimitive(lens.name.toUpperCase), 
        BlobPrimitive(lens.source.toString)
      ))
  }

  def alterView(name: String, query: Operator): Unit =
  {
    db.backend.update(s"UPDATE $viewTable SET query=? WHERE name=?", 
      List(
        BlobPrimitive(Serialization.serialize(query)),
        StringPrimitive(lens.name)
      )) 
  }

  def getView(name: String): Option[Operator] =
  {
    val results = 
      db.backend.resultRows(s"SELECT query FROM $viewTable WHERE name = ?", 
        List(StringPrimitive(name.toUpperCase))
      )
    encoded.flatten.headOption().map( Serialize.deserializeQuery(_) )
  }

  def listViews(): List[String] =
  {
    db.backend.resultRows(s"SELECT name FROM $viewTable").flatten
  }

}