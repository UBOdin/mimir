package mimir.parser

import sparsity.parser.{SQL,StreamParser}
import sparsity.statement.{Statement, Select}
import java.sql.SQLException
import java.io.Reader
import fastparse.Parsed

object MimirSQL
{
  
  def wrap(s: Parsed[Statement]): Parsed[MimirStatement] =
    s match {
      case Parsed.Success(q, idx) => Parsed.Success(SQLStatement(q), idx)
      case f:Parsed.Failure => f
    }

  def apply(input: Reader): Iterator[Parsed[MimirStatement]] = 
    SQL(input).map { wrap(_) }
  def apply(input: String): Parsed[MimirStatement] = 
    wrap(SQL(input))

  def select(input: String): Select =
    apply(input) match {
      case Parsed.Success(SQLStatement(select: Select), _) => select
      case _ => throw new SQLException(s"Invalid query $input")
    }

  def expression(input: String): sparsity.expression.Expression =
    sparsity.parser.Expression(input)

  
}