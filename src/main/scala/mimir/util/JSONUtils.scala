package mimir.util;

import play.api.libs.json._
import mimir.algebra._

object JSONUtils {

  def parsePrimitive(t: Type, jv: JsValue): PrimitiveValue =
  {
    (jv,t) match {
      case (JsNull, _)              => NullPrimitive()

      case (JsNumber(v), TInt())    => IntPrimitive(v.toInt)
      case (JsNumber(v), TFloat())  => FloatPrimitive(v.toDouble)
      case (JsNumber(v), TString()) => StringPrimitive(v.toString)
      case (JsNumber(_), _)         => throw new IllegalArgumentException(s"Invalid JSON ($jv) for Type $t")

      case (JsString(v), _)         => TextUtils.parsePrimitive(t, v)

      case (JsBoolean(v), TBool())  => BoolPrimitive(v)
      case (JsBoolean(v), _)        => throw new IllegalArgumentException(s"Invalid JSON ($jv) for Type $t")

      case (JsArray(_), _)          => throw new IllegalArgumentException(s"Invalid JSON ($jv) for Type $t")
      case (JsObject(_), _)         => throw new IllegalArgumentException(s"Invalid JSON ($jv) for Type $t")
    }
  }

}