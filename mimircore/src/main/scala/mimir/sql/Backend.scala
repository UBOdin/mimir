package mimir.sql;

import java.sql._

import mimir.algebra._
import net.sf.jsqlparser.statement.select.{Select, SelectBody};

abstract class Backend {
  def execute(sel: String): ResultSet
  def execute(sel: String, args: List[String]): ResultSet
  def execute(sel: Select): ResultSet = {
    execute(sel.toString());
  }
  def execute(selB: SelectBody): ResultSet = {
    val sel = new Select();
    sel.setSelectBody(selB);
    return execute(sel);
  }
  
  def getTableSchema(table: String): Option[List[(String, Type.T)]]
  def getTableOperator(table: String): Operator =
    getTableOperator(table, Map[String,Type.T]())
  def getTableOperator(table: String, metadata: Map[String, Type.T]): 
    Operator =
  {
    Table(
      table, 
      getTableSchema(table) match {
        case Some(x) => x.toMap
        case None => throw new SQLException("Table does not exist in db!")
      },
      metadata
    )
  }
  
  def update(stmt: String): Unit
  def update(stmt: String, args: List[String]): Unit

  def getAllTables(): List[String]
  def close(): Unit
}
