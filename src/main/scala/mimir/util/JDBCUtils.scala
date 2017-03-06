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
      case (java.sql.Types.DATE) => TDate()
      case (java.sql.Types.TIMESTAMP)   => TTimeStamp()
      case (java.sql.Types.VARCHAR |
            java.sql.Types.NULL |
            java.sql.Types.CHAR)     => TString()
      case (java.sql.Types.ROWID)    => TRowId()
    }
  }

  def convertMimirType(t: Type): Int = {
    t match {
      case TInt()       => java.sql.Types.INTEGER
      case TFloat()     => java.sql.Types.DOUBLE
      case TDate()      => java.sql.Types.DATE
      case TTimeStamp()  => java.sql.Types.TIMESTAMP
      case TString()    => java.sql.Types.VARCHAR
      case TRowId()     => java.sql.Types.ROWID
      case TAny()       => java.sql.Types.VARCHAR
      case TBool()      => java.sql.Types.INTEGER
      case TType()      => java.sql.Types.VARCHAR
      case TUser(t)     => convertMimirType(TypeRegistry.baseType(t))
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
        case TType() => 
          TypePrimitive(Type.fromString(
            results.getString(field)
          ))
        case TDate() =>
          val calendar = Calendar.getInstance()
          try {
            convertDate(results.getDate(field))
          } catch {
            case e: SQLException =>
              try {
                convertDate(Date.valueOf(results.getString(field)))
              } catch { case e: java.lang.IllegalArgumentException => new NullPrimitive }
            case e: NullPointerException =>
              new NullPrimitive
          }
        case TTimeStamp() =>
          try {
            convertTimeStamp(results.getTimestamp(field))
          } catch {
            case e: SQLException =>
              convertTimeStamp(Timestamp.valueOf(results.getString(field)))
            case e: NullPointerException =>
              new NullPrimitive
          }
        case TUser(t) => convertField(TypeRegistry.baseType(t), results, field)
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

  def convertTimeStamp(c: Calendar): TimestampPrimitive =
    TimestampPrimitive(c.get(Calendar.YEAR), c.get(Calendar.MONTH), c.get(Calendar.DATE),
                        c.get(Calendar.HOUR_OF_DAY), c.get(Calendar.MINUTE), c.get(Calendar.SECOND))
  def convertTimeStamp(ts: Timestamp): TimestampPrimitive =
  {
    val cal = Calendar.getInstance();
    cal.setTime(ts)
    convertTimeStamp(cal)
  }
  def convertTimeStamp(ts: TimestampPrimitive): Timestamp =
  {
    val cal = Calendar.getInstance()
    cal.set(ts.y, ts.m, ts.d, ts.hh, ts.mm, ts.ss);
    new Timestamp(cal.getTime().getTime());
  }

  def extractAllRows(results: ResultSet): JDBCResultSetIterable =
  {
    val meta = results.getMetaData()
    val schema = 
      (1 until (meta.getColumnCount() + 1)).map(
        colId => convertSqlType(meta.getColumnType(colId))
      ).toList
    extractAllRows(results, schema)    
  }

  def extractAllRows(results: ResultSet, schema: Seq[Type]): JDBCResultSetIterable =
  {
    new JDBCResultSetIterable(results, schema)
  }
}

class JDBCResultSetIterable(results: ResultSet, schema: Seq[Type]) 
  extends Iterator[Seq[PrimitiveValue]]
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
  def close(): Unit = { results.close() }

  def flush: Seq[Seq[PrimitiveValue]] = 
  { 
    val ret = toList
    close()
    return ret
  }
}