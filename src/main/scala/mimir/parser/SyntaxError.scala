package mimir.parser

case class SyntaxError(query: String, index: Int, lineNumber: Int, message: String)
{
  override def toString = s"$message (@ $lineNumber:$index)\nIn: $query\n${" "*(index+2)}^"
}
