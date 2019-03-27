package mimir.parser

case class SyntaxError(query: String, index: Int, message: String)
