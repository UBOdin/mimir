package mimir.util

import mimir.algebra._

object TextUtils {

  def parsePrimitive(t: Type, s: String): PrimitiveValue = 
  {
    t match {
      case TInt()    => IntPrimitive(java.lang.Long.parseLong(s))
      case TFloat()  => FloatPrimitive(java.lang.Double.parseDouble(s))
      case TDate()   => parseDate(s)
      case TTimeStamp() => parseTimeStamp(s)
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

  def parseDate(s: String): PrimitiveValue =
  {
    try {
      val fields = s.split("-").map( Integer.parseInt(_) )
      DatePrimitive(fields(0), fields(1), fields(2))
    } catch {
      case n: NumberFormatException => NullPrimitive()
    }
  }

  def parseTimeStamp(s: String): PrimitiveValue =
  {
    try {
      var initialFields = s.split("-") // format is YYYY-MM-DD hh:mm:ss
      val tempDayField = initialFields(2).split(" ") // timeStampFields(2) should be DD hh:mm:ss
      val tempTimeFields = tempDayField(1).split(":") //tempDayField(1) should be hh:mm:ss
      initialFields(2) = tempDayField(0)
      initialFields = initialFields ++ tempTimeFields

      val timeStampFields = initialFields.map(_.toInt)

      TimestampPrimitive(timeStampFields(0), timeStampFields(1), timeStampFields(1),
                      timeStampFields(1), timeStampFields(1), timeStampFields(1))
    } catch {
      case n: NumberFormatException => NullPrimitive()
    }
  }

}