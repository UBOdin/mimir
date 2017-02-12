package mimir.util;

import mimir.algebra.{PrimitiveValue,StringPrimitive};

object JSONBuilder {
	
	def list(content: Seq[String]): String =
		"["+content.mkString(",")+"]"

	def dict(content: Map[String,String]): String =
		dict(content.toList)

	def dict(content: Seq[(String,String)]): String =
		"{"+content.map( (x) => 
			"\""+x._1.toLowerCase()+"\":"+x._2
		).mkString(",")+"}"

	def string(content: String): String = {
		"\""+content.replace("\\", "\\\\").replace("\"", "\\\"")+"\""
	}

	def int(content: Int): String = {
		content.toString();
	}

	def double(content: Double): String = {
		content.toString();
	}

	def prim(content: PrimitiveValue) = {
		content match {
			case StringPrimitive(s) => string(s)
			case _ => content.toString()
		}
	}
}