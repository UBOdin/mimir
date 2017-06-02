package mimir.util;

import play.api.libs.json._
import mimir.algebra._

object JsonUtils {

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

  val dotPrefix = "\\.([^.\\[]+)".r
  val bracketPrefix = "\\[([0-9]+)\\]".r

  def seekPath(jv: JsValue, path: String): JsValue =
  {
    path match {

      case "" => return jv;

      case dotPrefix(arg) => {
        val jo:JsObject = jv.as[JsObject]
        seekPath(jo.value(arg), path.substring(arg.length + 1))
      }

      case bracketPrefix(arg) => {
        val ja:JsArray = jv.as[JsArray]
        seekPath(ja.value(Integer.parseInt(arg)), path.substring(arg.length + 2))
      }

      case _ =>
        throw new RAException(s"Invalid JSON Path Expression: '$path'")
    }
  }

  def toJson(p: PrimitiveValue): JsValue =
  {
    p match {
      case NullPrimitive() => JsNull
      case IntPrimitive(i) => JsNumber(i)
      case FloatPrimitive(f) => JsNumber(f)
      case StringPrimitive(s) => JsString(s)
      case BoolPrimitive(b) => JsBoolean(b)
      case DatePrimitive(y,m,d) => JsString(f"$y%04d-$m%02d-$d%02d")
      case TimestampPrimitive(y,m,d,hr,min,sec) => JsString(f"$y%04d-$m%02d-$d%02d $hr%02d:$min%02d:$sec%02d")
      case RowIdPrimitive(r) => JsString(r)
      case TypePrimitive(t) => JsString(t.toString)
    }
  }


}