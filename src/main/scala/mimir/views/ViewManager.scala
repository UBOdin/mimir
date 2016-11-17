package mimir.views;

import mimir._;

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
        StringPrimitive(lens.name), 
        BlobPrimitive(lens.source.toString)
      ))
  }

  def alterView(name: String, query: Operator): Unit =
  {
    db.backend.update(s"UPDATE $viewTable SET query=? WHERE name=?", 
      List(
        BlobPrimitive(lens.source.toString),
        StringPrimitive(lens.name)
      )) 
  }

  def listViews(): List[String] =
  {
    db.backend.resultRows(s"SELECT name FROM $")
  }

}