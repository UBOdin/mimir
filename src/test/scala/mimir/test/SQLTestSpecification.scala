package mimir.test

import java.io._

import sparsity.statement.Statement
import org.specs2.mutable._

import mimir._
import mimir.parser._
import mimir.sql._
import mimir.backend._
import mimir.algebra._
import mimir.util._
import mimir.exec._
import mimir.exec.result._
import mimir.optimizer._
import mimir.algebra.spark.OperatorTranslation
import mimir.ml.spark.SparkML
import org.specs2.specification.AfterAll

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
          val metadata = new JDBCMetadataBackend(jdbcBackendMode, tempDBName+".db")
          val backend:QueryBackend = 
            config.getOrElse("backend", "spark") match {
              case "spark" => new SparkBackend(tempDBName); 
            }
          val tmpDB = new Database(backend, metadata);
          if(shouldCleanupDB){    
            dbFile.deleteOnExit();
          }
          tmpDB.metadataBackend.open()
          tmpDB.backend.open();
          backend match {
            case sback:SparkBackend => SparkML(sback.sparkSql)
            case _ => ???
          }
          OperatorTranslation.db = tmpDB
          if(shouldResetDB || !oldDBExists){
            config.get("initial_db") match {
              case None => ()
              case Some(path) => Runtime.getRuntime().exec(s"cp $path $dbFile")
            }
          }
          tmpDB.initializeDBForMimir();
          if(shouldEnableInlining){
            metadata.enableInlining(tmpDB)
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
  with AfterAll
{
  //sequential
  def afterAll = {
    //TODO: There is likely a better way to do this:
    // hack for spark to delete all cached tables 
    // and temp views that may have shared names between tests
    try{
      if(config.getOrElse("cleanup", config.getOrElse("reset", "YES")) match { 
            case "NO" => false; case "YES" => true
          }) db.backend.dropDB()
      db.backend.close()
      db.backend.open()
    }catch {
      case t: Throwable => {}
    }
  }
  
  def dbFile = new File(tempDBName+".db")

  def db = DBTestInstances.get(tempDBName, config)

  def select(s: String) = 
    db.sqlToRA(MimirSQL.Select(s))
  def query[T](s: String)(handler: ResultIterator => T): T =
    db.query(select(s))(handler)
  def queryOneColumn[T](s: String)(handler: Iterator[PrimitiveValue] => T): T = 
    query(s){ result => handler(result.map(_(0))) }
  def querySingleton(s: String): PrimitiveValue =
    queryOneColumn(s){ _.next }
  def queryOneRow(s: String): Row =
    query(s){ _.next }
  def table(t: String) =
    db.table(t)
  def queryMetadata[T](s: String)(handler: ResultIterator => T): T =
    db.queryMetadata(select(s))(handler)
  def queryOneColumnMetadata[T](s: String)(handler: Iterator[PrimitiveValue] => T): T = 
    queryMetadata(s){ result => handler(result.map(_(0))) }
  def querySingletonMetadata(s: String): PrimitiveValue =
    queryOneColumnMetadata(s){ _.next }
  def queryOneRowMetadata(s: String): Row =
    queryMetadata(s){ _.next }
  def metadataTable(t: String) =
    db.metadataTable(ID(t))
  def resolveViews(q: Operator) =
    db.views.resolve(q)
  def explainRow(s: String, t: String) = 
  {
    val query = resolveViews(select(s))
    db.explainer.explainRow(query, RowIdPrimitive(t))
  }
  def explainCell(s: String, t: String, a:String) = 
  {
    val query = resolveViews(select(s))
    db.explainer.explainCell(query, RowIdPrimitive(t), ID(a))
  }
  def explainEverything(s: String) = 
  {
    val query = resolveViews(select(s))
    db.explainer.explainEverything(query)
  }
  def explainAdaptiveSchema(s: String) =
  {
    val query = resolveViews(select(s))
    db.explainer.explainAdaptiveSchema(query, query.columnNames.toSet, true)
  }  
  def update(s: MimirStatement) = 
    db.update(s)
  def update(s: String) = 
    db.update(stmt(s))
  def loadCSV(table: String, file: String) : Unit =
    LoadCSV.handleLoadTable(db, ID(table), file)
  def loadCSV(table: String, schema:Seq[(String,String)], file: String) : Unit =
    db.loadTable(
      targetTable = Some(ID(table)), 
      targetSchema = Some(schema.map { x => (ID.upper(x._1), Type.fromString(x._2)) }), 
      sourceFile = file 
    ) 
  def loadCSV(table: String, file: String, inferTypes:Boolean, detectHeaders:Boolean) : Unit =
    db.loadTable(
      targetTable = Some(ID(table)), 
      sourceFile = file,
      force = true,
      inferTypes = Some(inferTypes),
      detectHeaders = Some(detectHeaders)
    )
  def loadCSV(table: String, schema:Seq[(String,String)], file: String, inferTypes:Boolean, detectHeaders:Boolean) : Unit =
    db.loadTable(
      targetTable = Some(ID(table)), 
      sourceFile = file,
      targetSchema = Some(schema.map { x => (ID.upper(x._1), Type.fromString(x._2)) }), 
      force = true,
      inferTypes = Some(inferTypes),
      detectHeaders = Some(detectHeaders)
    )
    
  def modelLookup(model: String) = db.models.get(ID(model))
  def schemaLookup(table: String) = db.tableSchema(table).get
 }