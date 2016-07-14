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

  def convertField(t: Type.T, results: ResultSet, field: Integer): PrimitiveValue =
  {
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
    }
  }

  def extractAllRows(results: ResultSet): List[List[PrimitiveValue]] =
  {
    val meta = results.getMetaData()
    val schema = 
      (1 until (meta.getColumnCount() + 1)).map(
        colId => convertSqlType(meta.getColumnType(colId))
      )
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