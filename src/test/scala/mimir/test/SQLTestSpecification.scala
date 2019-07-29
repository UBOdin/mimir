package mimir.test

import java.io._

import sparsity.statement.Statement
import org.specs2.mutable._

import mimir._
import mimir.parser._
import mimir.sql._
import mimir.backend._
import mimir.metadata._
import mimir.algebra._
import mimir.util._
import mimir.exec._
import mimir.exec.result._
import mimir.optimizer._
import mimir.ml.spark.SparkML
import org.specs2.specification.AfterAll
import mimir.exec.spark._

object DBTestInstances
{
  private var databases = scala.collection.mutable.Map[String, Database]()
  private var sparkInitialized = false

  def initSpark
  {
    if(!sparkInitialized){
      MimirSpark.init(new MimirConfig(Seq()))
      sparkInitialized = true
    }
  }

  def get(tempDBName: String, config: Map[String,String]): Database =
  {
    this.synchronized { 
      this.initSpark

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
          // println("Preexists: "+dbFile.exists())
          if(shouldResetDB){
            if(dbFile.exists()){ dbFile.delete(); }
          }
          val oldDBExists = dbFile.exists();
          // println("Exists: "+oldDBExists)
          val metadata = new JDBCMetadataBackend(jdbcBackendMode, tempDBName+".db")
          val tmpDB = new Database(metadata);
          if(shouldCleanupDB){    
            dbFile.deleteOnExit();
          }
          tmpDB.open()
          if(shouldResetDB || !oldDBExists){
            config.get("initial_db") match {
              case None => ()
              case Some(path) => Runtime.getRuntime().exec(s"cp $path $dbFile")
            }
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
          }) 
      {
        for(view <- db.views.listTables){ 
          if(db.views(view).isMaterialized){ db.views.dematerialize(view) }
        }
        for(table <- db.loader.listTables){ 
          db.loader.drop(table) 
        }
      }
      db.close()
      //db.backend.open()
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
    query(s){ result => handler(result.map { row => row(0) }) }
  def querySingleton(s: String): PrimitiveValue =
    queryOneColumn(s){ _.next }
  def queryOneRow(s: String): Row =
    query(s){ _.next }
  def table(t: String) =
    db.table(t)
  def resolveViews(q: Operator) =
    db.views.resolve(q)
  def explainRow(s: String, t: String) = 
  {
    val query = resolveViews(select(s))
    db.uncertainty.explainRow(query, RowIdPrimitive(t))
  }
  def explainCell(s: String, t: String, a:String) = 
  {
    val query = resolveViews(select(s))
    db.uncertainty.explainCell(query, RowIdPrimitive(t), ID(a))
  }
  def explainEverything(s: String) = 
  {
    val query = resolveViews(select(s))
    db.uncertainty.explainEverything(query)
  }
  def explainAdaptiveSchema(s: String) =
  {
    val query = resolveViews(select(s))
    db.uncertainty.explainAdaptiveSchema(query, query.columnNames.toSet, true)
  }  
  def dropTable(t: String) =
    db.update(SQLStatement(sparsity.statement.DropTable(sparsity.Name(t), true)))
  def update(s: MimirStatement) = 
    db.update(s)
  def update(s: String) = 
    db.update(stmt(s))
  def loadCSV(
    sourceFile: String, 
    targetTable: String = null, 
    inferTypes:Boolean = true, 
    detectHeaders:Boolean = true, 
    targetSchema: Seq[String] = null
  ) : Unit =
    db.loader.loadTable(
      sourceFile = sourceFile,
      targetTable = Option(targetTable).map { ID(_) },
      inferTypes = Some(inferTypes),
      detectHeaders = Some(detectHeaders),
      targetSchema = Option(targetSchema).map { _.map { ID(_) } }
    )
    
  def modelLookup(model: String) = db.models.get(ID(model))
  def schemaLookup(table: String) = db.tableSchema(table).get
 }