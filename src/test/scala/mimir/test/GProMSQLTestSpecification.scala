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
import mimir.exec.result.ResultIterator
import mimir.exec.result.Row

object GProMDBTestInstances
{
  private var databases = scala.collection.mutable.Map[String, (Database, GProMBackend)]()
  
  def get(tempDBName: String, config: Map[String,String]): (Database, GProMBackend) =
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
          val backend = new GProMBackend(jdbcBackendMode, tempDBName+".db", 1)
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
          backend.metadataLookupPlugin.db = tmpDB;
          if(shouldResetDB || !oldDBExists || !config.contains("initial_db")){
            tmpDB.initializeDBForMimir();
          }
          if(shouldEnableInlining){
            backend.enableInlining(tmpDB)
          }
          mimir.gprom.algebra.OperatorTranslation.db = tmpDB
          databases.put(tempDBName, (tmpDB, backend))
          (tmpDB, backend)
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
abstract class GProMSQLTestSpecification(val tempDBName:String, config: Map[String,String] = Map())
  extends Specification
  with SQLParsers
  with RAParsers
{
  args.execute(threadsNb = 1)
  def dbFile = new File(tempDBName+".db")

  private def dbgp = GProMDBTestInstances.get(tempDBName, config)
  
  def db = dbgp._1
  
  def gp = dbgp._2
  
  def select(s: String) = {
    db.sql.convert(
      stmt(s).asInstanceOf[net.sf.jsqlparser.statement.select.Select]
    )
  }
  def query[T](s: String)(handler: ResultIterator => T): T =
    db.query(s)(handler)
  def queryOneColumn[T](s: String)(handler: Iterator[PrimitiveValue] => T): T = 
    query(s){ result => handler(result.map(_(0))) }
  def querySingleton(s: String): PrimitiveValue =
    queryOneColumn(s){ _.next }
  def queryOneRow(s: String): Row =
    query(s){ _.next }
  def table(t: String) =
    db.table(t)
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
  def loadCSV(file: String, table: String = null, delim:String = ",", allowAppend: Boolean = false, typeInference: Boolean = false, adaptiveSchema: Boolean = false) =
  {
    if(table == null){
      db.loadTable(
        new File(file),
        allowAppend = allowAppend,
        format = ("CSV", Seq(StringPrimitive(delim), BoolPrimitive(typeInference), BoolPrimitive(adaptiveSchema)))
      )
    } else {
      db.loadTable(
        new File(file),
        targetTable = table,
        allowAppend = allowAppend,
        format = ("CSV", Seq(StringPrimitive(delim), BoolPrimitive(typeInference), BoolPrimitive(adaptiveSchema)))
      )
    }
  }
}