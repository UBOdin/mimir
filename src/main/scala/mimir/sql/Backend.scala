package mimir.sql;

import java.sql._

import mimir.algebra._
import mimir.util.JDBCUtils
import net.sf.jsqlparser.statement.select.{Select, SelectBody};

abstract class Backend {
  def open(): Unit

  def execute(sel: String): ResultSet
  def execute(sel: String, args: List[PrimitiveValue]): ResultSet
  def execute(sel: Select): ResultSet = {
    execute(sel.toString());
  }
  def execute(selB: SelectBody): ResultSet = {
    val sel = new Select();
    sel.setSelectBody(selB);
    return execute(sel);
  }
  def resultRows(sel: String):Iterator[List[PrimitiveValue]] = 
    JDBCUtils.extractAllRows(execute(sel))
  def resultRows(sel: String, args: List[PrimitiveValue]):Iterator[List[PrimitiveValue]] =
    JDBCUtils.extractAllRows(execute(sel, args))
  def resultRows(sel: Select):Iterator[List[PrimitiveValue]] =
    JDBCUtils.extractAllRows(execute(sel))
  def resultRows(sel: SelectBody):Iterator[List[PrimitiveValue]] =
    JDBCUtils.extractAllRows(execute(sel))
  
  def getTableSchema(table: String): Option[List[(String, Type.T)]]

  
  def update(stmt: String): Unit
  def update(stmt: List[String]): Unit
  def update(stmt: String, args: List[PrimitiveValue]): Unit

  def getAllTables(): List[String]

  def close()

  def specializeQuery(q: Operator): Operator

}
