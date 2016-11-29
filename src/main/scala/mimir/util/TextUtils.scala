package mimir.util

import mimir.algebra._

object TextUtils {
  
  def parsePrimitive(t: Type.T, s: String): PrimitiveValue = 
  {
    t match {
      case Type.TInt    => IntPrimitive(java.lang.Long.parseLong(s))
      case Type.TFloat  => FloatPrimitive(java.lang.Double.parseDouble(s))
      case Type.TDate   => parseDate(s)
      case Type.TString => StringPrimitive(s)
      case Type.TBool   => 
        s.toUpperCase match {
          case "YES" | "TRUE"  | "1" => BoolPrimitive(true)
          case "NO"  | "FALSE" | "0" => BoolPrimitive(false)
        }
      case Type.TRowId  => RowIdPrimitive(s)
      case Type.TType   => TypePrimitive(Type.fromString(s))
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

}