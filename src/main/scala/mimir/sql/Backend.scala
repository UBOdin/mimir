package mimir.sql;

import java.sql._;
import java.io._;

import net.sf.jsqlparser.statement.select.Select;
import mimir.algebra._;

abstract class Backend {
  def execute(sel: String): ResultSet
  def execute(sel: String, args: List[String]): ResultSet
  def execute(sel: Select): ResultSet = {
    execute(sel.toString());
  }
  def execute(oper: Operator): ResultSet = {
    val sel = new Select();
    sel.setSelectBody(new RAToSql(this).convert(oper))
    execute(sel)
  }
  
  def getTableSchema(table: String): List[(String, Type.T)];
  
  def update(stmt: String): Unit
  def update(stmt: String, args: List[String]): Unit
}
