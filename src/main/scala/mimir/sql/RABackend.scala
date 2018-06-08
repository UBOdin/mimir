package mimir.sql

import mimir.algebra._
import org.apache.spark.sql.DataFrame
import mimir.Database

abstract class RABackend(val database:String) {
  def open(): Unit

  def materializeView(name:String): Unit
  def createTable(tableName:String, oper:Operator): Unit
  def execute(compiledOp: Operator): DataFrame
  

  /*def resultRows(sel: String):Seq[Seq[PrimitiveValue]] = 
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
    resultRows(sel).head.head*/
  
  def readDataSource(name:String, format:String, options:Map[String, String], schema:Option[Seq[(String, Type)]], load:Option[String]) : Unit
  
  
  def getTableSchema(table: String): Option[Seq[(String, Type)]]
  
  
  def getAllTables(): Seq[String]
  def invalidateCache();

  def close()

  def canHandleVGTerms: Boolean
  def rowIdType: Type
  def dateType: Type
  def specializeQuery(q: Operator, db: Database): Operator

  def listTablesQuery: Operator
  def listAttrsQuery: Operator

}