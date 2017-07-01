package mimir.util

import mimir.algebra._

object TextUtils {

  def parsePrimitive(t: Type, s: String): PrimitiveValue = 
  {
    t match {
      case TInt()    => IntPrimitive(java.lang.Long.parseLong(s))
      case TFloat()  => FloatPrimitive(java.lang.Double.parseDouble(s))
      case TDate()   => parseDate(s)
      case TTimestamp() => parseTimestamp(s)
      case TString() => StringPrimitive(s)
      case TBool()   => 
        s.toUpperCase match {
          case "YES" | "TRUE"  | "1" => BoolPrimitive(true)
          case "NO"  | "FALSE" | "0" => BoolPrimitive(false)
        }
      case TRowId()  => RowIdPrimitive(s)
      case TType()   => TypePrimitive(Type.fromString(s))
      case TAny()    => throw new RAException("Can't cast string to TAny")
      case TUser(t)  => parsePrimitive(TypeRegistry.baseType(t), s)
    }
  }

  val dateRegexp = "(\\d+)-(\\d+)-(\\d+)".r
  val timestampRegexp = "(\\d+)-(\\d+)-(\\d+) (\\d+):(\\d+):(\\d+|\\d+[.]\\d*)".r
  val intervalRegexp = "P(\\d+)Y(\\d+)M(\\d+)W(\\d+)DT(\\d+)H(\\d+)M(\\d+|\\d+[.]\\d*)S".r

  def parseDate(s: String): PrimitiveValue =
  {
    s match {
      case dateRegexp(y, m, d) => 
        DatePrimitive(y.toInt, m.toInt, d.toInt)
      case _ => NullPrimitive()
    }
  }

  def parseTimestamp(s: String): PrimitiveValue =
  {
    s match {
      case timestampRegexp(yr, mo, da, hr, mi, se) => 
        val seconds = se.toDouble
        TimestampPrimitive(yr.toInt, mo.toInt, da.toInt, hr.toInt, mi.toInt, seconds.toInt, (seconds*1000).toInt % 1000)
      case _ => NullPrimitive()
    }
  }

  def parseInterval(s: String): PrimitiveValue =
  {
    s match {
      case intervalRegexp(y, m, w, d, hh, mm, se) => 
        val seconds = se.toDouble
        IntervalPrimitive(y.toInt, m.toInt, w.toInt, d.toInt, hh.toInt, mm.toInt, seconds.toInt, (seconds*1000).toInt % 1000)
      case _ => NullPrimitive()
    }
  }
}
