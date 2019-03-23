package mimir.backend

import java.sql._
import mimir.Database
import mimir.algebra._
import mimir.util.JDBCUtils
import sparsity.statement.{Select => SelectStatement}
import sparsity.select.SelectBody

abstract class MetadataBackend {

  def open(): Unit

  def execute(sel: String): ResultSet
  def execute(sel: String, args: Seq[PrimitiveValue]): ResultSet
  def execute(sel: SelectStatement): ResultSet = {
    execute(sel.toString());
  }
  def execute(selB: SelectBody): ResultSet = 
    execute(SelectStatement(selB));
  def execute(sel: SelectStatement, args: Seq[PrimitiveValue]): ResultSet = {
    execute(sel.toString(), args);
  }
  def execute(selB: SelectBody, args: Seq[PrimitiveValue]): ResultSet = 
    execute(SelectStatement(selB), args);

  def resultRows(sel: String):Seq[Seq[PrimitiveValue]] = 
    JDBCUtils.extractAllRows(execute(sel)).flush
  def resultRows(sel: String, args: Seq[PrimitiveValue]):Seq[Seq[PrimitiveValue]] =
    JDBCUtils.extractAllRows(execute(sel, args)).flush
  def resultRows(sel: SelectStatement):Seq[Seq[PrimitiveValue]] =
    JDBCUtils.extractAllRows(execute(sel)).flush
  def resultRows(sel: SelectBody):Seq[Seq[PrimitiveValue]] =
    JDBCUtils.extractAllRows(execute(sel)).flush

  def resultValue(sel:String):PrimitiveValue =
    resultRows(sel).head.head
  def resultValue(sel:String, args: Seq[PrimitiveValue]):PrimitiveValue =
    resultRows(sel, args).head.head
  def resultValue(sel:SelectStatement):PrimitiveValue =
    resultRows(sel).head.head
  def resultValue(sel:SelectBody):PrimitiveValue =
    resultRows(sel).head.head
  
  def getTableSchema(table: ID): Option[Seq[(ID, Type)]]
  
  def update(stmt: sparsity.statement.Statement): Unit = update(stmt.toString)
  def update(stmt: String): Unit
  def update(stmt: TraversableOnce[String]): Unit
  def update(stmt: sparsity.statement.Statement, args: Seq[PrimitiveValue]): Unit = update(stmt.toString, args)
  def update(stmt: String, args: Seq[PrimitiveValue]): Unit
  def fastUpdateBatch(stmt: sparsity.statement.Statement, argArray: TraversableOnce[Seq[PrimitiveValue]]): Unit = fastUpdateBatch(stmt.toString, argArray)
  def fastUpdateBatch(stmt: String, argArray: TraversableOnce[Seq[PrimitiveValue]]): Unit
  def selectInto(table: ID, query: String): Unit

  def getAllTables(): Seq[ID]
  def invalidateCache();

  def close()

  def canHandleVGTerms: Boolean
  def rowIdType: Type
  def dateType: Type
  def specializeQuery(q: Operator, db: Database): Operator

  def listTablesQuery: Operator
  def listAttrsQuery: Operator

}

trait InlinableBackend {
	def enableInlining(db: Database) : Unit
}
