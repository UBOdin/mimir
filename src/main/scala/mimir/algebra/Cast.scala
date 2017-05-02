package mimir.algebra

import mimir.util._

object Cast 
{
  def apply(t: Type, x: PrimitiveValue): PrimitiveValue =
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
        case TTimeStamp()       => TextUtils.parseTimeStamp(x.asString)
        case TRowId()           => RowIdPrimitive(x.asString)
        case TAny()             => x
        case TBool()            => BoolPrimitive(x.asLong != 0)
        case TType()            => TypePrimitive(Type.fromString(x.asString))
        case TUser(name) => {
          val (typeRegexp, baseT) = TypeRegistry.registeredTypes(name)
          val base = apply(baseT, x) 
          if(typeRegexp.findFirstMatchIn(base.asString).isEmpty){
            NullPrimitive()
          } else {
            base
          }
        }
      }
    } catch {
      case _:TypeException=> NullPrimitive();
      case _:NumberFormatException => NullPrimitive();
    }
  }

  def apply(t: Type, x: String): PrimitiveValue =
    apply(t, StringPrimitive(x))
}