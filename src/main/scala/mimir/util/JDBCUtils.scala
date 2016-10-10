package mimir.util

import java.sql._
import java.util.{Calendar, GregorianCalendar}

import mimir.algebra._
import mimir.lenses._

object JDBCUtils {

  def convertSqlType(t: Int): Type = {
    t match {
      case (java.sql.Types.FLOAT |
            java.sql.Types.DECIMAL |
            java.sql.Types.REAL |
            java.sql.Types.DOUBLE |
            java.sql.Types.NUMERIC)   => TFloat()
      case (java.sql.Types.INTEGER)  => TInt()
      case (java.sql.Types.DATE |
            java.sql.Types.TIMESTAMP)     => TDate()
      case (java.sql.Types.VARCHAR |
            java.sql.Types.NULL |
            java.sql.Types.CHAR)     => TString()
      case (java.sql.Types.ROWID)    => TRowId()
    }
  }

  def convertMimirType(t: Type): Int = {
    t match {
      case TInt()    => java.sql.Types.INTEGER
      case TFloat()  => java.sql.Types.DOUBLE
      case TDate()   => java.sql.Types.DATE
      case TString() => java.sql.Types.VARCHAR
      case TRowId()  => java.sql.Types.ROWID
//      case Type.TUser   => java.sql.Types.VARCHAR
    }
  }

  def convertField(t: Type, results: ResultSet, field: Integer): PrimitiveValue =
  {
    val ret =
      t match {
        case TAny() =>
          convertField(
              convertSqlType(results.getMetaData().getColumnType(field)),
              results, field
            )
        case TFloat() =>
          FloatPrimitive(results.getDouble(field))
        case TInt() =>
          IntPrimitive(results.getLong(field))
        case TString() =>
          StringPrimitive(results.getString(field))
        case TRowId() =>
          RowIdPrimitive(results.getString(field))
        case TBool() =>
          BoolPrimitive(results.getInt(field) != 0)
        case TDate() =>
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
        case TUser(name,regex,sqlType) =>
          sqlType match {
            case TAny() =>
              convertField(
                convertSqlType(results.getMetaData().getColumnType(field)),
                results, field
              )
            case TFloat() =>
              FloatPrimitive(results.getDouble(field))
            case TInt() =>
              IntPrimitive(results.getLong(field))
            case TString() =>
              StringPrimitive(results.getString(field))
            case TRowId() =>
              RowIdPrimitive(results.getString(field))
            case TBool() =>
              BoolPrimitive(results.getInt(field) != 0)
            case TDate() =>
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
            case _ =>
              throw new Exception("In JDBCUtils expected one of the generic types but instead got: " + sqlType.toString)
          }

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

  def extractAllRows(results: ResultSet, schema: List[Type]): List[List[PrimitiveValue]] =
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

}