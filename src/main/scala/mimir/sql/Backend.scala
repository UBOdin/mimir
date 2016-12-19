package mimir.sql;

import java.sql._

import mimir.algebra._
import mimir.util.JDBCUtils
import net.sf.jsqlparser.statement.select.{Select, SelectBody};

abstract class Backend {
  def open(): Unit

  def execute(sel: String): ResultSet
  def execute(sel: String, args: Seq[PrimitiveValue]): ResultSet
  def execute(sel: Select): ResultSet = {
    execute(sel.toString());
  }
  def execute(selB: SelectBody): ResultSet = {
    val sel = new Select();
    sel.setSelectBody(selB);
    return execute(sel);
  }

  def resultRows(sel: String):Iterator[Seq[PrimitiveValue]] = 
    JDBCUtils.extractAllRows(execute(sel))
  def resultRows(sel: String, args: Seq[PrimitiveValue]):Iterator[Seq[PrimitiveValue]] =
    JDBCUtils.extractAllRows(execute(sel, args))
  def resultRows(sel: Select):Iterator[Seq[PrimitiveValue]] =
    JDBCUtils.extractAllRows(execute(sel))
  def resultRows(sel: SelectBody):Iterator[Seq[PrimitiveValue]] =
    JDBCUtils.extractAllRows(execute(sel))

  def resultValue(sel:String):PrimitiveValue =
    resultRows(sel).next.head
  def resultValue(sel:String, args: Seq[PrimitiveValue]):PrimitiveValue =
    resultRows(sel, args).next.head
  def resultValue(sel:Select):PrimitiveValue =
    resultRows(sel).next.head
  def resultValue(sel:SelectBody):PrimitiveValue =
    resultRows(sel).next.head
  
  def getTableSchema(table: String): Option[Seq[(String, Type)]]
  
  def update(stmt: String): Unit
  def update(stmt: TraversableOnce[String]): Unit
  def update(stmt: String, args: Seq[PrimitiveValue]): Unit

  def getAllTables(): Seq[String]

  def close()

  def specializeQuery(q: Operator): Operator

}
