package mimir.metadata;

import java.sql._

import mimir.Database
import mimir.algebra._
import mimir.util.JDBCUtils
import mimir.backend.sqlite._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import com.typesafe.scalalogging.slf4j.LazyLogging

class JDBCMetadataBackend(val protocol: String, val filename: String)
  extends MetadataBackend
  with LazyLogging
{
  var conn: Connection = null
  var openConnections = 0
  var inliningAvailable = false;

  def driver() = protocol

  val tableSchemas: scala.collection.mutable.Map[ID, Seq[(ID, Type)]] = mutable.Map()

  def open() = 
  {
    this.synchronized({
      assert(openConnections >= 0)
      if (openConnections == 0) {
        conn = protocol match {
          case "sqlite" =>
            Class.forName("org.sqlite.JDBC")
            val path = java.nio.file.Paths.get(filename).toString
            var c = java.sql.DriverManager.getConnection("jdbc:sqlite:" + path)
            SQLiteCompat.registerFunctions(c)
            c

          case x =>
            println("Unsupported backend! Exiting..."); System.exit(-1); null
        }
      }

      assert(conn != null)
      openConnections = openConnections + 1
    })
  }

  def close(): Unit = {
    this.synchronized({
      if (openConnections > 0) {
        openConnections = openConnections - 1
        if (openConnections == 0) {
          conn.close()
          conn = null
        }
      }

      assert(openConnections >= 0)
      if (openConnections == 0) assert(conn == null)
    })
  }

  val ID_COLUMN = "MIMIR_ID"

  def register(category: ID, fields: Seq[(ID, Type)])
  {
    // Assert that the backend schema lines up with the target
    // This should trigger a migration in the future, but at least 
    // inject a sanity check for now.
    protocol match {
      case "sqlite" => {
        // SQLite doesn't recognize anything more than the simplest possible types.
        // Type information is persisted but not interpreted, so conn.getMetaData() 
        // is useless for getting schema information.  Instead, we need to use a
        // SQLite-specific PRAGMA operation.
        SQLiteCompat.getTableSchema(conn, category) match {
          case Some(existing) => {
            assert(existing.length == fields.length)
            for(((_, e), f) <- existing.zip(fields)) {
              assert(Type.rootType(e) == f) 
            }
          }
          case None => {
            val create = s"CREATE TABLE ${category.quoted}("+
              (  
                Seq(s"${ID_COLUMN} string PRIMARY KEY NOT NULL")++
                fields.map { case (name, t) => s"${name.quoted} ${Type.rootType(t)}"}
              ).mkString(",")+
            ")"
            val stmt = conn.createStatement()
            stmt.executeUpdate(create)
            stmt.close()
          }
        }

      }
    }
    tableSchemas.put(category, fields)
  }

  def keys(category: ID): Seq[ID] = 
  {
    val select = s"SELECT ${ID_COLUMN} FROM ${category.quoted}"
    val stmt = conn.createStatement()
    val results = stmt.executeQuery(select)
    var keys:List[ID] = Nil 
    while(results.next){
      keys = ID(JDBCUtils.convertField(TString(), results, 1).asString) :: keys
    }
    results.close()
    return keys.reverse
  }
  def all(category: ID): Seq[(ID, Seq[PrimitiveValue])] = 
  {
    val fields = tableSchemas.get(category).get
    val select = "SELECT "+
      (Seq(ID_COLUMN)++fields.map { _._1.quoted }).mkString(",")+
      " FROM "+category.quoted;
    val stmt = conn.createStatement()
    val results = stmt.executeQuery(select)
    var resources:List[(ID, Seq[PrimitiveValue])] = Nil 
    while(results.next){
      val resource = (
        ID(JDBCUtils.convertField(TString(), results, 1).asString),
        fields.zipWithIndex.map {
          case ((_, t), idx) => JDBCUtils.convertField(t, results, 2+idx)
        }
      )
      resources = resource :: resources
    }
    results.close()
    return resources.reverse
  }
  def get(category: ID, resource: ID): Option[Seq[PrimitiveValue]] =
  {
    val fields = tableSchemas.get(category).get
    val select = "SELECT "+
      fields.map { _._1.quoted }.mkString(",")+
      " FROM "+category.quoted +
      " WHERE "+ID_COLUMN+" = "+StringPrimitive(resource.id);
    val stmt = conn.createStatement()
    val results = stmt.executeQuery(select)
    if(results.next){
      val ret = 
        fields.zipWithIndex.map {
          case ((_, t), idx) => JDBCUtils.convertField(t, results, 2+idx)
        }
      results.close()
      return Some(ret)
    } else {
      results.close()
      return None
    }
  }
  def put(category: ID, resource: ID, values: Seq[PrimitiveValue])
  {
    val fields = tableSchemas.get(category).get
    val upsert = s"INSERT OR REPLACE INTO ${category.quoted}("+
      ( 
        Seq(ID_COLUMN) ++ fields.map { _._1.quoted }
      ).mkString(",")+
    ") VALUES ("+( 0 until (fields.length+1) ).map { _ => "?" }.mkString(",")+")"
    val stmt = conn.prepareStatement(upsert)
    JDBCUtils.setArgs(stmt, Seq(StringPrimitive(resource.id))++values)
    stmt.execute()
    stmt.close()
  }
  
}