package mimir.algebra;

import mimir.algebra.Type._;
import org.joda.time.DateTime;

/////////////// Primitive Values ///////////////


/**
 * Slightly more specific base type for constant terms.  PrimitiveValue
 * also acts as a boxing type for constants in Mimir.
 */
abstract sealed class PrimitiveValue(t: Type)
  extends LeafExpression with Serializable
{
  def getType = t
  /**
   * Convert the current object into a long or throw a TypeException if 
   * not possible
   */
  def asLong: Long;
  /**
   * Convert the current object into an int or throw a TypeException if
   * not possible
   */
  def asInt: Int = asLong.toInt
  /**
   * Convert the current object into a double or throw a TypeException if 
   * not possible
   */
  def asDouble: Double;
  /**
   * Convert the current object into a float or throw a TypeException if 
   * not possible
   */
  def asFloat: Float = asDouble.toFloat
  /**
   * Convert the current object into a boolean or throw a TypeException if 
   * not possible
   */
  def asBool: Boolean;
  /**
   * Convert the current object into a DateTime or throw a TypeException if
   * not possible
   */
  def asDateTime: DateTime;
  /**
   * Convert the current object into a string or throw a TypeException if 
   * not possible.  Note the difference between this and toString.
   * asString returns the content, while toString returns a representation
   * of the primitive value itself.
   * An overt example of this is:
   *   val temp = StringPrimitive('foo')
   *   println(temp.asString)  // Returns "foo"
   *   println(temp.toString)  // Returns "'foo'"
   * Note the extra quotes.  If you ever see a problem involving strings
   * with ''too many nested quotes'', your problem is probably with asString
   */
  def asString: String;
  /**
   * return the contents of the variable as just an object.
   */
  def payload: Object;
}

abstract sealed class NumericPrimitive(t: Type) extends PrimitiveValue(t)

/**
 * Boxed representation of a long integer
 */
@SerialVersionUID(100L)
case class IntPrimitive(v: Long) 
  extends NumericPrimitive(TInt())
{
  override def toString() = v.toString
  def asLong: Long = v;
  def asDouble: Double = v.toDouble;
  def asBool: Boolean = throw new TypeException(TInt(), TBool(), "Hard Cast")
  def asDateTime: DateTime = throw new TypeException(TInt(), TDate(), "Hard Cast")
  def asString: String = v.toString;
  def payload: Object = v.asInstanceOf[Object];
}
/**
 * Boxed representation of a string
 */
@SerialVersionUID(100L)
case class StringPrimitive(v: String) 
  extends PrimitiveValue(TString())
{
  override def toString() = "'"+v.toString+"'"
  def asLong: Long = java.lang.Long.parseLong(v)
  def asDouble: Double = java.lang.Double.parseDouble(v)
  def asBool: Boolean = throw new TypeException(TString(), TBool(), "Hard Cast")
  def asDateTime: DateTime = DateTime.parse(v)
  def asString: String = v;
  def payload: Object = v.asInstanceOf[Object];
}
/**
 * Boxed representation of a type object
 */
@SerialVersionUID(100L)
case class TypePrimitive(t: Type)
  extends PrimitiveValue(TType())
{
  override def toString() = t.toString
  def asLong: Long = throw new TypeException(TType(), TInt(), "Hard Cast")
  def asDouble: Double = throw new TypeException(TType(), TFloat(), "Hard Cast")
  def asBool: Boolean = throw new TypeException(TType(), TBool(), "Hard Cast")
  def asDateTime: DateTime = throw new TypeException(TType(), TDate(), "Hard Cast")
  def asString: String = t.toString;
  def payload: Object = t.asInstanceOf[Object];
}
/**
 * Boxed representation of a row identifier/provenance token
 */
@SerialVersionUID(100L)
case class RowIdPrimitive(v: String)
  extends PrimitiveValue(TRowId())
{
  override def toString() = "'"+v.toString+"'"
  def asLong: Long = java.lang.Long.parseLong(v)
  def asDouble: Double = java.lang.Double.parseDouble(v)
  def asBool: Boolean = throw new TypeException(TRowId(), TBool(), "Hard Cast")
  def asDateTime: DateTime = throw new TypeException(TRowId(), TDate(), "Hard Cast")
  def asString: String = v;
  def payload: Object = v.asInstanceOf[Object];
}
/**
 * Boxed representation of a double-precision floating point number
 */
@SerialVersionUID(100L)
case class FloatPrimitive(v: Double) 
  extends NumericPrimitive(TFloat())
{
  override def toString() = v.toString
  def asLong: Long = throw new TypeException(TFloat(), TInt(), "Hard Cast");
  def asDouble: Double = v
  def asBool: Boolean = throw new TypeException(TFloat(), TBool(), "Hard Cast")
  def asDateTime: DateTime = throw new TypeException(TFloat(), TDate(), "Hard Cast")
  def asString: String = v.toString;
  def payload: Object = v.asInstanceOf[Object];
}

/**
 * Boxed representation of a date
 */
@SerialVersionUID(100L)
case class DatePrimitive(y: Int, m: Int, d: Int)
  extends PrimitiveValue(TDate())
{
  override def toString() = s"DATE '${asString}'"
  //def asLong: Long = throw new TypeException(TDate(), TInt(), "Hard Cast");
  def asLong : Long = new DateTime(y,m,d,0,0,0).getMillis
  def asDouble: Double = throw new TypeException(TDate(), TFloat(), "Hard Cast");
  def asBool: Boolean = throw new TypeException(TDate(), TBool(), "Hard Cast")
  def asString: String = f"$y%04d-$m%02d-$d%02d"
  def asInterval : Long = new DateTime(y,m,d,0,0,0).getMillis
  def payload: Object = (y, m, d).asInstanceOf[Object];
  final def compare(c: DatePrimitive): Integer = {
    if(c.y < y){ -1 }
    else if(c.y > y) { 1 }
    else if(c.m < m) { -1 }
    else if(c.m > m) { 1 }
    else if(c.d < d) { -1 }
    else if(c.d > d) { 1 }
    else { 0 }
  }

  def >(c:DatePrimitive): Boolean = compare(c) > 0
  def >=(c:DatePrimitive): Boolean = compare(c) >= 0
  def <(c:DatePrimitive): Boolean = compare(c) < 0
  def <=(c:DatePrimitive): Boolean = compare(c) <= 0

  def asDateTime: DateTime = new DateTime(y, m, d, 0, 0)
}

/**
  *
  * Boxed Representation of Timestamp
  */
@SerialVersionUID(100L)
case class TimestampPrimitive(y: Int, m: Int, d: Int, hh: Int, mm: Int, ss: Int)
  extends PrimitiveValue(TTimestamp())
{
  override def toString() = s"DATE '${asString}'"
  //def asLong: Long = throw new TypeException(TDate(), TInt(), "Hard Cast");
  def asLong : Long = new DateTime(y, m, d, hh, mm, ss).getMillis;
  def asDouble: Double = throw new TypeException(TDate(), TFloat(), "Hard Cast");
  def asBool: Boolean = throw new TypeException(TDate(), TBool(), "Hard Cast")
  def asString: String = f"$y%04d-$m%02d-$d%02d $hh%02d:$mm%02d:$ss%02d"
  def asInterval: Long = new DateTime(y, m, d, hh, mm, ss).getMillis;
  def payload: Object = (y, m, d).asInstanceOf[Object];
  final def compare(c: TimestampPrimitive): Integer = {
    if(c.y < y){ -1 }
    else if(c.y > y) { 1 }
    else if(c.m < m) { -1 }
    else if(c.m > m) { 1 }
    else if(c.d < d) { -1 }
    else if(c.d > d) { 1 }
    else if(c.hh < hh) { -1 }
    else if(c.hh > hh) { 1 }
    else if(c.mm < mm) { -1 }
    else if(c.mm > mm) { 1 }
    else if(c.ss < ss) { -1 }
    else if(c.ss > ss) { 1 }
    else { 0 }
  }

  def >(c:TimestampPrimitive): Boolean = compare(c) > 0
  def >=(c:TimestampPrimitive): Boolean = compare(c) >= 0
  def <(c:TimestampPrimitive): Boolean = compare(c) < 0
  def <=(c:TimestampPrimitive): Boolean = compare(c) <= 0

  def asDateTime: DateTime = new DateTime(y, m, d, hh, mm, ss)
}
/**
 * Boxed representation of a boolean
 */
@SerialVersionUID(100L)
case class BoolPrimitive(v: Boolean)
  extends PrimitiveValue(TBool())
{
  override def toString() = if(v) {"TRUE"} else {"FALSE"}
  def asLong: Long = throw new TypeException(TBool(), TInt(), "Hard Cast");
  def asDouble: Double = throw new TypeException(TBool(), TFloat(), "Hard Cast");
  def asDateTime: DateTime = throw new TypeException(TBool(), TDate(), "Hard Cast")
  def asBool: Boolean = v
  def asString: String = toString;
  def payload: Object = v.asInstanceOf[Object];
}
/**
 * Boxed representation of NULL
 */
@SerialVersionUID(100L)
case class NullPrimitive()
  extends PrimitiveValue(TAny())
{
  override def toString() = "NULL"
  def asLong: Long = throw new NullTypeException(TAny(), TInt(), "Hard Cast Null");
  def asDouble: Double = throw new NullTypeException(TAny(), TFloat(), "Hard Cast Null");
  def asString: String = throw new NullTypeException(TAny(), TString(), "Hard Cast Null");
  def asBool: Boolean = throw new NullTypeException(TAny(), TBool(), "Hard Cast Null")
  def asDateTime: DateTime = throw new NullTypeException(TAny(), TDate(), "Hard Cast")
  def payload: Object = null
}
/**
 * Boxed representation of Interval
 */
@SerialVersionUID(100L)
case class IntervalPrimitive(ms: Long)
  extends PrimitiveValue(TInterval())
{
  override def toString() = ms.toString
  def asLong: Long = ms;
  def asDouble: Double = ms;
  def asString: String = "'"+ms.toString+"'";
  def asBool: Boolean = throw new NullTypeException(TAny(), TBool(), "Hard Cast Null")
  def asInterval: Long = ms;
  def asDateTime: DateTime = new DateTime(1970,1,1,0,0,0).plusMillis(ms.toInt)
  def payload: Object = null
}

