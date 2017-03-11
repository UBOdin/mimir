package mimir.test

import java.io._

import net.sf.jsqlparser.statement.{Statement}
import org.specs2.mutable._

import mimir._
import mimir.parser._
import mimir.sql._
import mimir.algebra._
import mimir.util._
import mimir.exec._

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
          val shouldEnableInlining = config.getOrElse("inline", "YES") match { 
            case "NO" => false; case "YES" => true
          }
          val oldDBExists = dbFile.exists();
          val backend = new JDBCBackend(jdbcBackendMode, tempDBName+".db")
          val tmpDB = new Database(backend);
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
          if(shouldResetDB && !oldDBExists && !config.contains("initial_db")){
            tmpDB.initializeDBForMimir();
          }
          if(shouldEnableInlining){
            backend.enableInlining(tmpDB)
          }
          databases.put(tempDBName, tmpDB)
          tmpDB
        }
      }
    }
  }
}

/**
 * Generic superclass for a test.
 * 
 * TODO: Turn this into a trait or set of traits
 */
abstract class SQLTestSpecification(val tempDBName:String, config: Map[String,String] = Map())
  extends Specification
  with SQLParsers
{

  var history:List[Operator] = Nil

  def dbFile = new File(tempDBName+".db")

  def db = DBTestInstances.get(tempDBName, config)

  def select(s: String) = {
    stmt(s) match {
      case sel:net.sf.jsqlparser.statement.select.Select => 
        db.sql.convert(sel)
    }
  }
  def query(s: String): ResultIterator = {
    val query = select(s)
    history = query :: history
    db.query(query)
  }
  def queryOneColumn(s: String): Iterable[PrimitiveValue] = 
    query(s).mapRows(_(0))
  def querySingleton(s: String): PrimitiveValue =
    queryOneColumn(s).head
  def queryOneRow(s: String): Iterable[PrimitiveValue] =
    query(s).mapRows( _.currentRow ).head
  def table(t: String) =
    db.getTableOperator(t)
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
  def update(s: Statement) = 
    db.update(s)
  def update(s: String) = 
    db.update(stmt(s))
  def loadCSV(table: String, file: File) =
    LoadCSV.handleLoadTable(db, table, file)
  def parser = new OperatorParser(db.models.get, db.getTableSchema(_).get)
  def expr = parser.expr _
  def oper = parser.operator _
  def i = IntPrimitive(_:Long).asInstanceOf[PrimitiveValue]
  def f = FloatPrimitive(_:Double).asInstanceOf[PrimitiveValue]
  def str = StringPrimitive(_:String).asInstanceOf[PrimitiveValue]
}