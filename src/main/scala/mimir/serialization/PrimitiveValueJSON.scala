package mimir.serialization

import play.api.libs.json._ // JSON library
import play.api.libs.json.Reads._ // Custom validation helpers
import play.api.libs.functional.syntax._ // Combinator syntax

import mimir.algebra._

object PrimitiveValueJSON { 
  implicit val intPrimitiveReads   : Reads[IntPrimitive   ] = JsPath.read[Long]  .map { IntPrimitive(_) }
  implicit val floatPrimitiveReads : Reads[FloatPrimitive ] = JsPath.read[Double].map { FloatPrimitive(_) }
  implicit val stringPrimitiveReads: Reads[StringPrimitive] = JsPath.read[String].map { StringPrimitive(_) }
  implicit val typePrimitiveReads  : Reads[TypePrimitive  ] = JsPath.read[String].map { Type.fromString(_) }.map { TypePrimitive(_) }
  implicit val rowidPrimitiveReads : Reads[RowIdPrimitive ] = JsPath.read[String].map { RowIdPrimitive(_) }
  implicit val boolPrimitiveReads  : Reads[BoolPrimitive] = (
    JsPath.read[Boolean] or
    JsPath.read[String].map { _.toLowerCase }.map { 
      case "yes" | "true" => true
      case "no" | "false" => false
    }
  ).map { BoolPrimitive(_) }
  implicit val datePrimitiveReads  : Reads[DatePrimitive] = (
    (JsPath \ "year").read[Int] and
    (JsPath \ "month").read[Int] and
    (JsPath \ "date").read[Int]
  )( DatePrimitive.apply _ )
  implicit val timePrimitiveReads  : Reads[TimestampPrimitive] = (
    (JsPath \ "year").read[Int] and
    (JsPath \ "month").read[Int] and
    (JsPath \ "date").read[Int] and
    (JsPath \ "hour").readNullable[Int].map { _.getOrElse(0) } and
    (JsPath \ "min").readNullable[Int].map { _.getOrElse(0) } and
    (JsPath \ "sec").readNullable[Int].map { _.getOrElse(0) } and
    (JsPath \ "msec").readNullable[Int].map { _.getOrElse(0) }
  )( TimestampPrimitive.apply _ )
  implicit val primitiveValueReads : Reads[PrimitiveValue] = (
    JsPath.read[FloatPrimitive].map { _.asInstanceOf[PrimitiveValue] } or 
    JsPath.read[Boolean].map { BoolPrimitive(_).asInstanceOf[PrimitiveValue] } or 
    JsPath.read[TimestampPrimitive].map { _.asInstanceOf[PrimitiveValue] } or
    JsPath.read[StringPrimitive].map { _.asInstanceOf[PrimitiveValue] }
  )

  implicit val primitiveValueWrites = new Writes[PrimitiveValue] { def writes(p:PrimitiveValue) = p match {
    case _:NullPrimitive => JsNull
    case x:IntPrimitive => JsNumber(x.v)
    case x:FloatPrimitive => JsNumber(x.v)
    case x:StringPrimitive => JsString(x.v)
    case x:TypePrimitive => JsString(x.t.toString)
    case x:RowIdPrimitive => JsString(x.v)
    case x:BoolPrimitive => JsBoolean(x.v)
    case DatePrimitive(y, m, d) => JsObject(Map[String,JsValue](
      "year" -> JsNumber(y),
      "month" -> JsNumber(m),
      "date" -> JsNumber(d)
    ))
    case TimestampPrimitive(y, m, d, hh, mm, ss, ms) => JsObject(Map[String,JsValue](
      "year"  -> JsNumber(y),
      "month" -> JsNumber(m),
      "date"  -> JsNumber(d),
      "hour"  -> JsNumber(hh),
      "min"   -> JsNumber(mm),
      "sec"   -> JsNumber(ss),
      "msec"  -> JsNumber(ms)
    ))
    case x:IntervalPrimitive => JsString(x.toString)
  }}

  implicit val primitiveValueFormat = Format[PrimitiveValue](primitiveValueReads, primitiveValueWrites)


}
