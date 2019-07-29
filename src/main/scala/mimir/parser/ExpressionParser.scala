package mimir.parser

import java.sql.SQLException
import fastparse._
import sparsity.parser.{ Expression => SparsityExpr }
import mimir.algebra._
import mimir.sql.SqlToRA


object ExpressionParser {
	def apply = expr _

	def expr(input: String): Expression =
		parse(input, SparsityExpr.expression(_)) match {
      case Parsed.Success(expr, _) => SqlToRA(expr, SqlToRA.literalBindings(_))
      case Parsed.Failure(msg, idx, extra) => throw new SQLException(s"Invalid expression (failure @ $idx: ${extra.trace().longMsg}) $input")
		}

	def list(input: String): Seq[Expression] = 
		parse(input, SparsityExpr.expressionList(_)) match {
      case Parsed.Success(exprList, _) => exprList.map { SqlToRA(_, SqlToRA.literalBindings(_)) }
      case Parsed.Failure(msg, idx, extra) => throw new SQLException(s"Invalid expression list (failure @ $idx: ${extra.trace().longMsg}) $input")
		}

	def prim(input: String): PrimitiveValue = 
		parse(input, SparsityExpr.primitive(_)) match {
      case Parsed.Success(prim, _) => SqlToRA(prim)
      case Parsed.Failure(msg, idx, extra) => throw new SQLException(s"Invalid primitive (failure @ $idx: ${extra.trace().longMsg}) $input")
		}

	def function(input: String): Function =
		parse(input, SparsityExpr.function(_)) match {
      case Parsed.Success(sparsity.expression.Function(name, args, _), _) => 
      	Function(ID.lower(name), args.toSeq.flatten.map { SqlToRA(_, SqlToRA.literalBindings(_)) })
      case Parsed.Failure(msg, idx, extra) => throw new SQLException(s"Invalid function (failure @ $idx: ${extra.trace().longMsg}) $input")

		}
}