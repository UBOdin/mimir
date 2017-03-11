package mimir.test

import java.io._
import net.sf.jsqlparser.statement.{Statement}
import mimir.parser._

trait SQLParsers {

  def stmts(f: String): List[Statement] = 
    stmts(new File(f))
  def stmts(f: File): List[Statement] = {
    val p = new MimirJSqlParser(new FileReader(f))
    var ret = List[Statement]();
    var s: Statement = null;

    do{
      s = p.Statement()
      if(s != null) {
        ret = s :: ret;
      }
    } while(s != null)
    ret.reverse
  }
  def stmt(s: String) = {
    new MimirJSqlParser(new StringReader(s)).Statement()
  }
  def selectStmt(s: String) = {
    stmt(s).asInstanceOf[net.sf.jsqlparser.statement.select.Select]
  }

  def sqlSimpleExpr(s:String) = {
    new MimirJSqlParser(new StringReader(s)).SimpleExpression()
  }
  def sqlBoolExpr(s:String) = {
    new MimirJSqlParser(new StringReader(s)).Expression()
  }

}