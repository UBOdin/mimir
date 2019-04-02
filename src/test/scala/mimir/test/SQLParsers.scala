package mimir.test

import java.io._
import mimir.parser._
import sparsity.statement.Statement

trait SQLParsers {

  def stmts(f: String): Seq[MimirStatement] = 
    stmts(new File(f))
  def stmts(f: File): Seq[MimirStatement] = {
    val p = new EndlessParser()
    p.load(f);
    p.iterator.map { 
      case SQLCommand(cmd) => cmd
      case cmd => throw new Exception(s"invalid statement: $cmd")
    }.toSeq
  }
  def stmt(s: String) =
    MimirSQL.Get(s)

  def selectStmt(s: String) = 
    MimirSQL.Select(s)

  def sqlSimpleExpr(s:String) = 
    sparsity.parser.Expression(s)
  def sqlBoolExpr(s:String) = 
    sparsity.parser.Expression(s)

}