package mimir.algebra

import mimir.util._

object Cast 
{
  def apply(t: BaseType, x: PrimitiveValue): PrimitiveValue =
  {
    try {
      t match {
        case TInt()             => IntPrimitive(x.asLong)
        case TFloat()           => FloatPrimitive(x.asDouble)
        case TString()          => StringPrimitive(x.asString)
        case TDate()            => 
          x match { 
            case _:DatePrimitive => x
            case _ => TextUtils.parseDate(x.asString)
          }
        case TTimestamp()       => 
          x match { 
            case _:TimestampPrimitive => x
            case _ => TextUtils.parseTimestamp(x.asString)
          }
      	case TInterval()	=> 
          x match { 
            case _:IntervalPrimitive => x
            case _ => TextUtils.parseInterval(x.asString)
          }
        case TRowId()           => RowIdPrimitive(x.asString)
        case TBool()            => BoolPrimitive(x.asLong != 0)
        case TType()            => TypePrimitive(
                                      BaseType.fromString(x.asString)
                                              .getOrElse{ TUser(x.asString) } 
                                   )
        case TAny()             => x
      }
    } catch {
      case _:TypeException=> NullPrimitive();
      case _:NumberFormatException => NullPrimitive();
    }
  }

  def apply(t: BaseType, x: String): PrimitiveValue =
    apply(t, StringPrimitive(x))
}
