package mimir.test

import java.io._
import mimir.parser._
import sparsity.statement.Statement

trait SQLParsers {

  def stmts(f: String): Seq[MimirStatement] = 
    stmts(new File(f))
  def stmts(f: File): Seq[MimirStatement] =
    MimirSQL.Get(new FileReader(f)).toSeq
  def stmt(s: String) =
    MimirSQL.Get(s)

  def selectStmt(s: String) = 
    MimirSQL.Select(s)

  def sqlSimpleExpr(s:String) = 
    sparsity.parser.Expression(s)
  def sqlBoolExpr(s:String) = 
    sparsity.parser.Expression(s)

}