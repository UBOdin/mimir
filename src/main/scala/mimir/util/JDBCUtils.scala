package mimir.util

import java.sql._
import java.util.{Calendar, GregorianCalendar}

import mimir.algebra._
import mimir.lenses._
import java.text.SimpleDateFormat

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
      case (java.sql.Types.TIMESTAMP)   => TTimestamp()
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
      case TTimestamp()  => java.sql.Types.TIMESTAMP
      case TString()    => java.sql.Types.VARCHAR
      case TRowId()     => java.sql.Types.ROWID
      case TAny()       => java.sql.Types.VARCHAR
      case TBool()      => java.sql.Types.INTEGER
      case TType()      => java.sql.Types.VARCHAR
      case TInterval()  => java.sql.Types.VARCHAR
      case TUser(t)     => convertMimirType(TypeRegistry.baseType(t))
    }
  }


  def convertFunction(t: Type, field: Integer, dateType: Type = TDate()): (ResultSet => PrimitiveValue) =
  {
    val checkNull: ((ResultSet, => PrimitiveValue) => PrimitiveValue) = {
      (r, call) => {
        val ret = call
        if(r.wasNull()){ NullPrimitive() }
        else { ret }
      }
    }

    t match {
      case TAny() =>        if(ExperimentalOptions.isEnabled("XNULL")) (r) => NullPrimitive() else throw new SQLException(s"Can't extract TAny: $field")
      case TFloat() =>      (r) => checkNull(r, { FloatPrimitive(r.getDouble(field)) })
      case TInt() =>        (r) => checkNull(r, { IntPrimitive(r.getLong(field)) })
      case TString() =>     (r) => checkNull(r, { StringPrimitive(r.getString(field)) })
      case TRowId() =>      (r) => checkNull(r, { RowIdPrimitive(r.getString(field)) })
      case TBool() =>       (r) => checkNull(r, { convertBool(r.getString(field)) })
      case TType() =>       (r) => checkNull(r, { TypePrimitive(Type.fromString(r.getString(field))) })
      case TDate() =>
        dateType match {
          case TDate() =>   (r) => { val d = r.getDate(field); if(d == null){ NullPrimitive() } else { convertDate(d) } }
          case TString() => (r) => { 
              val d = r.getString(field)
              if(d == null){ NullPrimitive() } 
              else { TextUtils.parseDate(d) }
            }
          case _ =>         throw new SQLException(s"Can't extract TDate as $dateType")
        }
      case TTimestamp() => 
        dateType match {
          case TDate() =>   (r) => { 
              val t = r.getTimestamp(field); 
              if(t == null){ NullPrimitive() } 
              else { convertTimestamp(t) } 
            }
          case TString() => (r) => {
              val t = r.getString(field)
              if(t == null){ NullPrimitive() }
              else { TextUtils.parseTimestamp(t) }
            }
          case _ =>         throw new SQLException(s"Can't extract TTimestamp as $dateType")

        }
      case TInterval() => (r) => { TextUtils.parseInterval(r.getString(field)) }
      case TUser(t) => convertFunction(TypeRegistry.baseType(t), field, dateType)
    }
  }

  def convertField(t: Type, results: ResultSet, field: Integer, rowIdType: Type = TString()): PrimitiveValue =
  {
    convertFunction(
      t match {
        case TAny() => convertSqlType(results.getMetaData().getColumnType(field))
        case _ => t
      }, 
      field, 
      rowIdType
    )(results)
  }

  def convertBool(boolStr:String) : PrimitiveValue = {
    if(boolStr.forall(_.isDigit)){
      BoolPrimitive(boolStr.toLong != 0)
    }
    else boolStr.toLowerCase() match {
      case "true" => BoolPrimitive( true )
      case "false" => BoolPrimitive( false )
      case _ => NullPrimitive()
    }     
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
  def convertTimestamp(c: Calendar): TimestampPrimitive =
    TimestampPrimitive(c.get(Calendar.YEAR), c.get(Calendar.MONTH), c.get(Calendar.DATE),
                        c.get(Calendar.HOUR_OF_DAY), c.get(Calendar.MINUTE), c.get(Calendar.SECOND), 
                        c.get(Calendar.MILLISECOND))
  def convertTimestamp(ts: Timestamp): TimestampPrimitive =
  {
    val cal = Calendar.getInstance();
    cal.setTime(ts)
    convertTimestamp(cal)
  }
  def convertTimestamp(ts: TimestampPrimitive): Timestamp =
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
