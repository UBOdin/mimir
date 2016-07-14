package mimir.sql;

import java.sql._

import mimir.algebra._
import net.sf.jsqlparser.statement.select.{Select, SelectBody};

abstract class Backend {
  def open(): Unit

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
    getTableOperator(table, List[(String,Expression,Type.T)]())
  def getTableOperator(table: String, metadata: List[(String, Expression, Type.T)]):
    Operator =
  {
    Table(
      table, 
      getTableSchema(table) match {
        case Some(x) => x
        case None => throw new SQLException("Table does not exist in db!")
      },
      metadata
    )
  }
  
  def update(stmt: String): Unit
  def update(stmt: List[String]): Unit
  def update(stmt: String, args: List[String]): Unit

  def getAllTables(): List[String]

  def close()

}
