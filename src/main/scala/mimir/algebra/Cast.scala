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
        case TDate()            => TextUtils.parseDate(x.asString)
        case TRowId()           => RowIdPrimitive(x.asString)
        case TAny()             => x
        case TBool()            => BoolPrimitive(x.asLong != 0)
        case TType()            => TypePrimitive(Type.fromString(x.asString))
        case TUser(_, regex, t2) => {
          val base = apply(t2, x) 
          if((base.asString.matches(regex))){
            base
          } else {
            NullPrimitive()
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