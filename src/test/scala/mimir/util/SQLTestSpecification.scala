package mimir.util

import java.io._

import net.sf.jsqlparser.statement.{Statement}
import org.specs2.mutable._

import mimir._
import mimir.parser._
import mimir.sql._
import mimir.algebra._

object DBTestInstances
{
  private var databases = scala.collection.mutable.Map[String, Database]()

  def get(tempDBName: String, config: Map[String,String]): Database =
  {
    this.synchronized { 
      databases.get(tempDBName) match { 
        case Some(db) => db
        case None => {
          val dbFile = new File (tempDBName+".db")
          val jdbcBackendMode:String = config.getOrElse("jdbc", "sqlite")
          val shouldResetDB = config.getOrElse("reset", "YES") match { 
            case "NO" => false; case "YES" => true
          }
          val oldDBExists = dbFile.exists();
          val tmpDB = new Database(tempDBName, new JDBCBackend(jdbcBackendMode, tempDBName+".db"));
          if(shouldResetDB){
            if(dbFile.exists()){ dbFile.delete(); }
          }
          config.get("initial_db") match {
            case None => ()
            case Some(path) => Runtime.getRuntime().exec(s"cp $path $dbFile")
          }
          if(shouldResetDB){    
            dbFile.deleteOnExit();
          }
          tmpDB.backend.open();
          if(shouldResetDB || !oldDBExists || !config.contains("initial_db")){
            tmpDB.initializeDBForMimir();
          }
          databases.put(tempDBName, tmpDB)
          tmpDB
        }
      }
    }
  }
}


abstract class SQLTestSpecification(val tempDBName:String, config: Map[String,String] = Map())
  extends Specification
{

  def dbFile = new File(tempDBName+".db")

  def db = DBTestInstances.get(tempDBName, config)

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
  def parser = new OperatorParser(db.lenses.modelForLens, db.getTableSchema(_).get)
  def expr = parser.expr _
  def oper = parser.operator _
  def i = IntPrimitive(_:Long).asInstanceOf[PrimitiveValue]
  def f = FloatPrimitive(_:Double).asInstanceOf[PrimitiveValue]
  def str = StringPrimitive(_:String).asInstanceOf[PrimitiveValue]
}