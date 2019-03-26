package mimir.parser

import fastparse._, NoWhitespace._
import fastparse.Parsed
import sparsity.parser.StreamParser
import java.io.Reader

sealed abstract class MimirCommand

case class SlashCommand(
  body: String
) extends MimirCommand

case class SQLCommand(
  body: MimirStatement
) extends MimirCommand

object MimirCommand
{
  def apply(input: Reader): Iterator[Parsed[MimirCommand]] = 
    new StreamParser[MimirCommand](
      parse(_:Iterator[String], command(_)), 
      input
    )
  def apply(input: String): Parsed[MimirCommand] = 
    parse(input, command(_))
  
  def command[_:P]: P[MimirCommand] = P(
      slashCommand 
    | ( MimirSQL.statement.map { SQLCommand(_) } ~ ";" )
  )

  def slashCommand[_:P] = P(
    "\\" ~
    CharsWhile( 
      c => (c != '\n') && (c != '\r') 
    ).!.map { SlashCommand(_) }
  )
}