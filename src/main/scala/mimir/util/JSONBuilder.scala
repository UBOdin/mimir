package mimir.util;

import mimir.algebra.{PrimitiveValue,StringPrimitive,TypePrimitive};
import play.api.libs.json.JsString
import play.api.libs.json.JsArray
import play.api.libs.json.JsObject
import play.api.libs.json.JsValue
import play.api.libs.json.JsNumber
import play.api.libs.json.JsNull
import play.api.libs.json.JsBoolean
import mimir.algebra.NullPrimitive
import mimir.algebra.RowIdPrimitive
import mimir.algebra.IntPrimitive
import mimir.algebra.FloatPrimitive
import mimir.algebra.BoolPrimitive
import mimir.algebra.TimestampPrimitive
import mimir.algebra.DatePrimitive


object JSONBuilder {
	
	def list(content: Seq[Any]): String =
		listJs(content).toString()

  private def listJs(content: Seq[Any]): JsArray =
		JsArray(content.map { el => value(el) })
		
	def dict(content: Map[String,Any]): String =
		dictJs(content).toString()

	def dict(content: Seq[(String,Any)]): String =
		JsObject(content.map( (x) => x._1.toLowerCase() -> value(x._2))).toString()

  private def dictJs(content: Map[String,Any]): JsObject =
		JsObject(content.map { el => el._1 -> value(el._2) } )
		
	def string(content: String): String = {
		value(content).toString()
	}

	def int(content: Int): String = {
		value(content).toString()
	}

	def double(content: Double): String = {
		value(content).toString()
	}

	def prim(content: PrimitiveValue) = {
		primJs(content).toString()
	}
	
	private def primJs(content: PrimitiveValue) = {
		content match {
			case StringPrimitive(s) => JsString(s)
			case TypePrimitive(t) => JsString(t.toString())
			case NullPrimitive() => JsNull
      case RowIdPrimitive(s) => JsString(s)
      case IntPrimitive(i) => JsNumber(i)
      case FloatPrimitive(f) => JsNumber(f)
      case BoolPrimitive(b) => JsBoolean(b)
      case TimestampPrimitive(_,_,_,_,_,_,_) => JsString(content.asString)
      case DatePrimitive(_,_,_) => JsString(content.asString)
      case _ =>  JsString(content.toString())
		}
	}
	
	private def value(content: Any): JsValue = {
		content match {
		  case s:String => {
		    try {
		      play.api.libs.json.Json.parse(s)
		    } catch {
		      case t: Throwable => JsString(s)
		    }
		  }
			case null => JsNull
      case i:Int => JsNumber(i)
      case f:Float => JsNumber(f)
      case l:Long => JsNumber(l)
      case d:Double => JsNumber(d)
      case b:Boolean => JsBoolean(b)
      case seq:Seq[Any] => listJs(seq)
      case map:Map[_,_] => dictJs(map.asInstanceOf[Map[String,Any]])
      case jsval:JsValue => jsval
      case prim:PrimitiveValue => primJs(prim)
			case _ =>  JsString(content.toString())
		}
	}
}