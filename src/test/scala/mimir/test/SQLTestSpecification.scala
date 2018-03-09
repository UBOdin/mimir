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
import mimir.exec.result._
import mimir.optimizer._
import mimir.algebra.spark.OperatorTranslation
import mimir.ml.spark.SparkML

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
          val shouldCleanupDB = config.getOrElse("cleanup", config.getOrElse("reset", "YES")) match { 
            case "NO" => false; case "YES" => true
          }
          val shouldEnableInlining = config.getOrElse("inline", "YES") match { 
            case "NO" => false; case "YES" => true
          }
          if(shouldResetDB){
            if(dbFile.exists()){ dbFile.delete(); }
          }
          val oldDBExists = dbFile.exists();
          // println("Exists: "+oldDBExists)
          val backend = new JDBCMetadataBackend(jdbcBackendMode, tempDBName+".db")
          val sback = new SparkBackend()
          val tmpDB = new Database(sback, backend);
          if(shouldCleanupDB){    
            dbFile.deleteOnExit();
          }
          tmpDB.metadataBackend.open()
          tmpDB.backend.open();
          SparkML(sback.sparkSql)
          OperatorTranslation.db = tmpDB
          if(shouldResetDB || !oldDBExists){
            config.get("initial_db") match {
              case None => ()
              case Some(path) => Runtime.getRuntime().exec(s"cp $path $dbFile")
            }
          }
          tmpDB.initializeDBForMimir();
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
  with RAParsers
{

  def dbFile = new File(tempDBName+".db")

  def db = DBTestInstances.get(tempDBName, config)

  def select(s: String) = {
    stmt(s) match {
      case sel:net.sf.jsqlparser.statement.select.Select => 
        db.sql.convert(sel)
    }
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
  def queryMetadata[T](s: String)(handler: ResultIterator => T): T =
    db.queryMetadata(s)(handler)
  def queryOneColumnMetadata[T](s: String)(handler: Iterator[PrimitiveValue] => T): T = 
    queryMetadata(s){ result => handler(result.map(_(0))) }
  def querySingletonMetadata(s: String): PrimitiveValue =
    queryOneColumnMetadata(s){ _.next }
  def queryOneRowMetadata(s: String): Row =
    queryMetadata(s){ _.next }
  def metadataTable(t: String) =
    db.metadataTable(t)
  def resolveViews(q: Operator) =
    db.views.resolve(q)
  def explainRow(s: String, t: String) = 
  {
    val query = resolveViews(db.sql.convert(
      stmt(s).asInstanceOf[net.sf.jsqlparser.statement.select.Select]
    ))
    db.explainRow(query, RowIdPrimitive(t))
  }
  def explainCell(s: String, t: String, a:String) = 
  {
    val query = resolveViews(db.sql.convert(
      stmt(s).asInstanceOf[net.sf.jsqlparser.statement.select.Select]
    ))
    db.explainCell(query, RowIdPrimitive(t), a)
  }
  def explainEverything(s: String) = 
  {
    val query = resolveViews(db.sql.convert(
      stmt(s).asInstanceOf[net.sf.jsqlparser.statement.select.Select]
    ))
    db.explainer.explainEverything(query)
  }
  def update(s: Statement) = 
    db.update(s)
  def update(s: String) = 
    db.update(stmt(s))
  def loadCSV(table: String, file: File) : Unit =
    LoadCSV.handleLoadTable(db, table, file)
  def loadCSV(table: String, schema:Seq[(String,String)], file: File) : Unit =
    loadCSVSchema(table, schema.map(el => (el._1, Type.fromString(el._2))), file)
  private def loadCSVSchema(table: String, schema:Seq[(String,Type)], file: File) : Unit =
    LoadCSV.handleLoadTableRaw(db, table, schema, file, Map())
    
  def modelLookup(model: String) = db.models.get(model)
  def schemaLookup(table: String) = db.tableSchema(table).get
 }