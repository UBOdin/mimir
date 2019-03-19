package mimir.parser

import sparsity.parser.{SQL,StreamParser}
import sparsity.statement.Select
import java.sql.SQLException

object MimirSQL
{
  
  def apply(input: Reader): Iterator[MimirStatement] = 
    SQL(input).map { SQLStatement(_) }
  def apply(input: String): MimirStatement = 
    SQLStatement(SQL(input))

  def select(input: String): Select =
    apply(input) match {
      case SQLStatement(select: Select) => select
      case _ => throw new SQLException(s"Invalid query $input")
    }

  def expression(input: String): sparsity.expresion.Expression =
    sparsity.parser.Expression(input)

  
}