package mimir.test

import java.io._
import java.sql.SQLException

import mimir.parser._
import sparsity.statement.Statement
import fastparse.Parsed

trait SQLParsers {

  def stmts(f: String): Seq[MimirStatement] = 
    stmts(new File(f))
  def stmts(f: File): Seq[MimirStatement] = {
    val p = MimirCommand(new FileReader(f))
    p.map { 
      case Parsed.Success(SQLCommand(cmd), _) => cmd
      case Parsed.Success(cmd:SlashCommand, _) => throw new SQLException(s"Expecting SQL, not command: $cmd")
      case fail:Parsed.Failure => throw new SQLException(fail.longMsg)
    }.toSeq
  }
  def stmt(s: String) =
    MimirSQL.Get(s)

  def selectStmt(s: String) = 
    MimirSQL.Select(s)

  def sqlExpr(s:String) = 
    sparsity.parser.Expression(s)

}