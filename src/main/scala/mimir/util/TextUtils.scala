package mimir.util

import com.typesafe.scalalogging.slf4j.LazyLogging
import mimir.algebra._

object TextUtils extends LazyLogging {

  def parsePrimitive(t: Type, s: String): PrimitiveValue = 
  {
    t match {
      case TInt()    => IntPrimitive(java.lang.Long.parseLong(s))
      case TFloat()  => FloatPrimitive(java.lang.Double.parseDouble(s))
      case TDate()   => parseDate(s)
      case TTimestamp() => parseTimestamp(s)
      case TInterval() => parseInterval(s)
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

  //val dateRegexp = """(\d+)[\\\/-](\d+)[\\\/-](\d+)""".r
  //val timestampRegexp = """(\d+)[\\\/-](\d+)[\\\/-](\d+) (\d+):(\d+):(\d+|\d+[.]\d*)""".r
  val dateRegexp = "(\\d+)-(\\d+)-(\\d+)".r
  val timestampRegexp = "(\\d+)-(\\d+)-(\\d+) (\\d+):(\\d+):(\\d+|\\d+[.]\\d*)".r
  val intervalRegexp = "P(\\d+)Y(\\d+)M(\\d+)W(\\d+)DT(\\d+)H(\\d+)M(\\d+|\\d+[.]\\d*)S".r

  def parseDate(s: String): PrimitiveValue =
  {
    logger.trace(s"Parse Date: '$s'")
    s match {
      case dateRegexp(y, m, d) => 
        logger.trace(s"   -> $y-$m-$d  -> ${DatePrimitive(y.toInt, m.toInt, d.toInt)}")
        DatePrimitive(y.toInt, m.toInt, d.toInt)
      case _ => NullPrimitive()
    }
  }

  def parseTimestamp(s: String): PrimitiveValue =
  {
    logger.trace(s"Parse Timestamp: '$s'")
    s match {
      case timestampRegexp(yr, mo, da, hr, mi, se) => 
        val seconds = se.toDouble
        TimestampPrimitive(yr.toInt, mo.toInt, da.toInt, hr.toInt, mi.toInt, seconds.toInt, (seconds*1000).toInt % 1000)
      case _ => NullPrimitive()
    }
  }

  def parseInterval(s: String): PrimitiveValue =
  {
    logger.trace(s"Parse Interval: '$s'")
    s match {
      case intervalRegexp(y, m, w, d, hh, mm, se) => 
        val seconds = se.toDouble
        IntervalPrimitive(new org.joda.time.Period(y.toInt, m.toInt, w.toInt, d.toInt, hh.toInt, mm.toInt, seconds.toInt, (seconds * 1000).toInt % 1000))
      case _ => NullPrimitive()
    }
  }
  
  object Levenshtein {
    def minimum(i1: Int, i2: Int, i3: Int)=scala.math.min(scala.math.min(i1, i2), i3)
    def distance(s1:String, s2:String)={
      val dist=Array.tabulate(s2.length+1, s1.length+1){(j,i)=>if(j==0) i else if (i==0) j else 0}
 
      for(j<-1 to s2.length; i<-1 to s1.length)
         dist(j)(i)=if(s2(j-1)==s1(i-1)) dist(j-1)(i-1)
	            else minimum(dist(j-1)(i)+1, dist(j)(i-1)+1, dist(j-1)(i-1)+1)
 
      dist(s2.length)(s1.length)
    }
  }
}

