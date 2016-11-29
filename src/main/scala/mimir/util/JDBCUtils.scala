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
    }
  }

  def convertMimirType(t: Type.T): Int = {
    t match {
      case Type.TInt    => java.sql.Types.INTEGER
      case Type.TFloat  => java.sql.Types.DOUBLE
      case Type.TDate   => java.sql.Types.DATE
      case Type.TString => java.sql.Types.VARCHAR
      case Type.TRowId  => java.sql.Types.ROWID
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
            convertDate(results.getDate(field))
          } catch {
            case e: SQLException =>
              convertDate(Date.valueOf(results.getString(field)))
            case e: NullPointerException =>
              new NullPrimitive
          }
        case Type.TBlob => {
          val blob = results.getBlob(field)
          BlobPrimitive(blob.getBytes(0, blob.length().toInt))
        }
      }
    if(results.wasNull()) { NullPrimitive() }
    else { ret }
  }

  def convertDate(c: Calendar): DatePrimitive =
    DatePrimitive(c.get(Calendar.YEAR), c.get(Calendar.MONTH), c.get(Calendar.DATE))
  def convertDate(d: Date): DatePrimitive =
  {
    val cal = Calendar.getInstance();
    cal.setTime(d)
    convertDate(cal)
  }
  def convertDate(d: DatePrimitive): Date =
  {
    val cal = Calendar.getInstance()
    cal.set(d.y, d.m, d.d);
    new Date(cal.getTime().getTime());
  }

  def extractAllRows(results: ResultSet): Iterator[List[PrimitiveValue]] =
  {
    val meta = results.getMetaData()
    val schema = 
      (1 until (meta.getColumnCount() + 1)).map(
        colId => convertSqlType(meta.getColumnType(colId))
      ).toList
    extractAllRows(results, schema)    
  }

  def extractAllRows(results: ResultSet, schema: List[Type.T]): Iterator[List[PrimitiveValue]] =
  {
    new JDBCResultSetIterable(results, schema)
  }

}


class JDBCResultSetIterable(results: ResultSet, schema: List[Type.T]) extends Iterator[List[PrimitiveValue]]
{
  def next(): List[PrimitiveValue] = 
  {
    while(results.isBeforeFirst()){ results.next(); }
    val ret = schema.
          zipWithIndex.
          map( t => JDBCUtils.convertField(t._1, results, t._2+1) ).
          toList
    results.next();
    return ret;
  }

  def hasNext(): Boolean = { return !results.isAfterLast() }
}