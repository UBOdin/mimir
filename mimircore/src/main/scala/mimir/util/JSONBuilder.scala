package mimir.util;

import mimir.algebra.{PrimitiveValue,StringPrimitive};

object JSONBuilder {
	
	def list(content: List[String]): String =
		"["+content.mkString(",")+"]"

	def dict(content: Map[String,String]): String =
		dict(content.toList)

	def dict(content: List[(String,String)]): String =
		"{"+content.map( (x) => 
			"'"+x._1.toLowerCase()+"':"+x._2
		).mkString(",")+"}"

	def string(content: String): String = {
		"'"+content.replace("\\", "\\\\").replace("'", "\\'")+"'"
	}

	def prim(content: PrimitiveValue) = {
		content match {
			case StringPrimitive(s) => string(s)
			case _ => content.toString()
		}
	}
}