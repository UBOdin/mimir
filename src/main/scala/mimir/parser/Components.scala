package mimir.parser

import fastparse._, NoWhitespace._

object Components { 

  def slashCommand[_:P] = P(
    "\\" ~
    CharsWhile( 
      c => (c != '\n') && (c != '\r') 
    ).!.map { SlashCommand(_) }
  )
}