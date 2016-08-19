package mimir.util

import java.sql._
import java.util.{GregorianCalendar, Calendar};
import mimir.algebra._

object JDBCUtils {

  def convertSqlType(t: Int): Type.T = { 
    t match {
      case (java.sql.Types.FLOAT |
            java.sql.Types.DECIMAL |
            java.sql.Types.REAL |
            java.sql.Types.DOUBLE |
            java.sql.Types.NUMERIC)   => Type.TFloat
      case (java.sql.Types.INTEGER)  => Type.TInt
      case (java.sql.Types.DATE |
            java.sql.Types.TIMESTAMP)     => Type.TDate
      case (java.sql.Types.VARCHAR |
            java.sql.Types.NULL |
            java.sql.Types.CHAR)     => Type.TString
      case (java.sql.Types.ROWID)    => Type.TRowId
      case (java.sql.Types.BLOB)     => Type.TBlob
    }
  }

  def convertMimirType(t: Type.T): Int = {
    t match {
      case Type.TInt    => java.sql.Types.INTEGER
      case Type.TFloat  => java.sql.Types.DOUBLE
      case Type.TDate   => java.sql.Types.DATE
      case Type.TString => java.sql.Types.VARCHAR
      case Type.TRowId  => java.sql.Types.ROWID
      case Type.TBlob   => java.sql.Types.BLOB
    }
  }

  def convertField(t: Type.T, results: ResultSet, field: Integer): PrimitiveValue =
  {
    val ret =
      t match {
        case Type.TAny => 
          convertField(
              convertSqlType(results.getMetaData().getColumnType(field)),
              results, field
            )
        case Type.TFloat =>
          FloatPrimitive(results.getDouble(field))
        case Type.TInt =>
          IntPrimitive(results.getLong(field))
        case Type.TString =>
          StringPrimitive(results.getString(field))
        case Type.TRowId =>
          RowIdPrimitive(results.getString(field))
        case Type.TBool =>
          BoolPrimitive(results.getInt(field) != 0)
        case Type.TDate => 
          val calendar = Calendar.getInstance()
          try {
            calendar.setTime(results.getDate(field))
          } catch {
            case e: SQLException =>
              calendar.setTime(Date.valueOf(results.getString(field)))
            case e: NullPointerException =>
              new NullPrimitive
          }
          DatePrimitive(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DATE))
        case Type.TBlob => 
          BlobPrimitive(results.getBytes(field))
      }
    if(results.wasNull()) { NullPrimitive() }
    else { ret }
  }

  def extractAllRows(results: ResultSet): List[List[PrimitiveValue]] =
  {
    val meta = results.getMetaData()
    val schema = 
      (1 until (meta.getColumnCount() + 1)).map(
        colId => convertSqlType(meta.getColumnType(colId))
      ).toList
    extractAllRows(results, schema)    
  }

  def extractAllRows(results: ResultSet, schema: List[Type.T]): List[List[PrimitiveValue]] =
  {
    var ret = List[List[PrimitiveValue]]()
    // results.first();
    while(results.isBeforeFirst()){ results.next(); }
    while(!results.isAfterLast()){
      ret = 
        schema.
          zipWithIndex.
          map( t => convertField(t._1, results, t._2+1) ).
          toList :: ret
      results.next()
    }
    ret.reverse
  }

  def extractSingleton(results: ResultSet): PrimitiveValue =
  {
    val meta = results.getMetaData()
    if(meta.getColumnCount() != 1){
      throw new SQLException("Expecting Single Column Result")
    }
    extractSingleton(results, convertSqlType(meta.getColumnType(1)))
  }
  def extractSingleton(results: ResultSet, t: Type.T): PrimitiveValue =
  {
    if(results.getMetaData().getColumnCount() != 1){
      throw new SQLException("Expecting Single Column Result")
    }
    while(results.isBeforeFirst()){ results.next(); }
    val ret = convertField(t, results, 1)
    results.next();
    if(!results.isAfterLast()){ 
      throw new SQLException("Expecting Single Row Result")
    }
    ret
  }

}