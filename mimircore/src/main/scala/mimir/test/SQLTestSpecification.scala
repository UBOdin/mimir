package mimir.test

import java.io._

import net.sf.jsqlparser.statement.{Statement}
import org.specs2.mutable._

import mimir._
import mimir.parser._
import mimir.sql._
import mimir.algebra._

abstract class SQLTestSpecification(val tempDBName:String, jdbcBackendMode:String = "sqlite")
  extends Specification
{
  val db = {
    val tmpDB = new Database(tempDBName, new JDBCBackend(jdbcBackendMode, tempDBName));
    if(dbFile.exists()){ dbFile.delete(); }
    dbFile.deleteOnExit();
    tmpDB.backend.open();
    tmpDB.initializeDBForMimir();
    tmpDB
  }


  def dbFile = new File(new File("databases"), tempDBName)


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
  def select(s: String) = {
    db.sql.convert(
      stmt(s).asInstanceOf[net.sf.jsqlparser.statement.select.Select]
    )
  }
  def query(s: String) = {
    val query = select(s)
    db.query(query)
  }
  def explainRow(s: String, t: String) = {
    val query = db.sql.convert(
      stmt(s).asInstanceOf[net.sf.jsqlparser.statement.select.Select]
    )
    db.explainRow(query, RowIdPrimitive(t))
  }
  def explainCell(s: String, t: String, a:String) = {
    val query = db.sql.convert(
      stmt(s).asInstanceOf[net.sf.jsqlparser.statement.select.Select]
    )
    db.explainCell(query, RowIdPrimitive(t), a)
  }
  def lens(s: String) =
    db.createLens(stmt(s).asInstanceOf[mimir.sql.CreateLens])
  def update(s: Statement) = 
    db.backend.update(s.toString())
  def parser = new ExpressionParser(db.lenses.modelForLens)
  def expr = parser.expr _
  def i = IntPrimitive(_:Long).asInstanceOf[PrimitiveValue]
  def f = FloatPrimitive(_:Double).asInstanceOf[PrimitiveValue]
  def str = StringPrimitive(_:String).asInstanceOf[PrimitiveValue]
}