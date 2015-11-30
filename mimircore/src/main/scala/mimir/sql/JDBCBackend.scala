package mimir.sql;

import java.sql._

import mimir.Methods
import mimir.algebra.Type

import scala.collection.mutable.ListBuffer
;

object JDBCUtils {
  def convertSqlType(t: Int): Type.T = { 
    t match {
      case (java.sql.Types.FLOAT |
            java.sql.Types.DECIMAL |
            java.sql.Types.REAL |
            java.sql.Types.DOUBLE |
            java.sql.Types.NUMERIC)   => Type.TFloat
      case (java.sql.Types.INTEGER)  => Type.TInt
      case (java.sql.Types.DATE |
            java.sql.Types.TIMESTAMP)     => Type.TDate
      case (java.sql.Types.VARCHAR |
            java.sql.Types.NULL |
            java.sql.Types.CHAR)     => Type.TString
      case (java.sql.Types.ROWID)    => Type.TRowId
    }
  }
}

class JDBCBackend(backend: String, filename: String) extends Backend
{
  var conn: Connection = null
  var openConnections = 0

  def driver() = backend


  def open() = {
    this.synchronized({
      assert(openConnections >= 0)
      if (openConnections == 0) {
        conn = backend match {
          case "sqlite" =>
            Class.forName("org.sqlite.JDBC")
            val path = java.nio.file.Paths.get("databases", filename).toString
            java.sql.DriverManager.getConnection("jdbc:sqlite:" + path)

          case "oracle" =>
            Methods.getConn()

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



  def execute(sel: String): ResultSet = 
  {
    //println(sel)
    if(conn == null) {
      throw new SQLException("Trying to use unopened connection!")
    }
    val stmt = conn.createStatement()
    val ret = stmt.executeQuery(sel)
    stmt.closeOnCompletion()
    ret
  }
  def execute(sel: String, args: List[String]): ResultSet = 
  {
    //println(""+sel+" <- "+args)
    if(conn == null) {
      throw new SQLException("Trying to use unopened connection!")
    }
    val stmt = conn.prepareStatement(sel)
    var i: Int = 0
    args.map( (a) => {
      i += 1
      stmt.setString(i, a)
    })
    stmt.executeQuery()
  }
  
  def update(upd: String): Unit =
  {
    if(conn == null) {
      throw new SQLException("Trying to use unopened connection!")
    }
    val stmt = conn.createStatement()
    stmt.executeUpdate(upd)
    stmt.close()
  }

  def update(upd: List[String]): Unit =
  {
    if(conn == null) {
      throw new SQLException("Trying to use unopened connection!")
    }
    val stmt = conn.createStatement()
    upd.indices.foreach(i => stmt.addBatch(upd(i)))
    stmt.executeBatch()
    stmt.close()
  }

  def update(upd: String, args: List[String]): Unit =
  {
    if(conn == null) {
      throw new SQLException("Trying to use unopened connection!")
    }
    val stmt = conn.prepareStatement(upd);
    var i: Int = 0
    args.map( (a) => {
      i += 1
      stmt.setString(i, a)
    })
    stmt.execute()
    stmt.close()
  }
  
  def getTableSchema(table: String): Option[List[(String, Type.T)]] =
  {
    if(conn == null) {
      throw new SQLException("Trying to use unopened connection!")
    }
    val tables = this.getAllTables().map{(x) => x.toUpperCase}
    if(!tables.contains(table.toUpperCase)) return None

    val cols = backend match {
      case "sqlite" => conn.getMetaData().getColumns(null, null, table, "%")
      case "oracle" => conn.getMetaData().getColumns(null, "ARINDAMN", table, "%")  // TODO Generalize
    }

    var ret = List[(String, Type.T)]()

    while(cols.isBeforeFirst()){ cols.next(); }
    while(!cols.isAfterLast()){
      ret = ret ++ List((
          cols.getString("COLUMN_NAME").toUpperCase, 
          JDBCUtils.convertSqlType(cols.getInt("DATA_TYPE"))
        ))
      cols.next()
    }
    cols.close()
    Some(ret)
  }

  def getAllTables(): List[String] = {
    if(conn == null) {
      throw new SQLException("Trying to use unopened connection!")
    }

    val metadata = conn.getMetaData()
    val tables = backend match {
      case "sqlite" => metadata.getTables(null, null, "%", null)
      case "oracle" => metadata.getTables(null, "ARINDAMN", "%", null) // TODO Generalize
    }

    val tableNames = new ListBuffer[String]()

    while(tables.next()) {
      tableNames.append(tables.getString("TABLE_NAME"))
    }

    tables.close()
    tableNames.toList
  }
}