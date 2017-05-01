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

  def resultRows(sel: String):Seq[Seq[PrimitiveValue]] = 
    JDBCUtils.extractAllRows(execute(sel)).flush
  def resultRows(sel: String, args: Seq[PrimitiveValue]):Seq[Seq[PrimitiveValue]] =
    JDBCUtils.extractAllRows(execute(sel, args)).flush
  def resultRows(sel: Select):Seq[Seq[PrimitiveValue]] =
    JDBCUtils.extractAllRows(execute(sel)).flush
  def resultRows(sel: SelectBody):Seq[Seq[PrimitiveValue]] =
    JDBCUtils.extractAllRows(execute(sel)).flush

  def resultValue(sel:String):PrimitiveValue =
    resultRows(sel).head.head
  def resultValue(sel:String, args: Seq[PrimitiveValue]):PrimitiveValue =
    resultRows(sel, args).head.head
  def resultValue(sel:Select):PrimitiveValue =
    resultRows(sel).head.head
  def resultValue(sel:SelectBody):PrimitiveValue =
    resultRows(sel).head.head
  
  def getTableSchema(table: String): Option[Seq[(String, Type)]]
  
  def update(stmt: String): Unit
  def update(stmt: TraversableOnce[String]): Unit
  def update(stmt: String, args: Seq[PrimitiveValue]): Unit
  def fastUpdateBatch(stmt: String, argArray: TraversableOnce[Seq[PrimitiveValue]]): Unit
  def selectInto(table: String, query: String): Unit

  def getAllTables(): Seq[String]
  def invalidateCache();

  def close()

  def canHandleVGTerms: Boolean
  def rowIdType: Type
  def dateType: Type
  def specializeQuery(q: Operator): Operator

  def listTablesQuery: Operator
  def listAttrsQuery: Operator

}
