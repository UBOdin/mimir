package mimir.sql;

import java.sql._

import mimir.Methods
import mimir.algebra._
import mimir.util.JDBCUtils
import mimir.sql.sqlite._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
;

class JDBCBackend(backend: String, filename: String) extends Backend
{
  var conn: Connection = null
  var openConnections = 0

  def driver() = backend

  val tableSchemas: scala.collection.mutable.Map[String, List[(String, Type.T)]] = mutable.Map()

  def open() = {
    this.synchronized({
      assert(openConnections >= 0)
      if (openConnections == 0) {
        conn = backend match {
          case "sqlite" =>
            Class.forName("org.sqlite.JDBC")
            val path = java.nio.file.Paths.get("databases", filename).toString
            var c = java.sql.DriverManager.getConnection("jdbc:sqlite:" + path)
            SQLiteCompat.registerFunctions(c)
            c

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
    try {
      if(conn == null) {
        throw new SQLException("Trying to use unopened connection!")
      }
      val stmt = conn.createStatement()
      val ret = stmt.executeQuery(sel)
      stmt.closeOnCompletion()
      ret
    } catch { 
      case e: SQLException => println(e.toString+"during\n"+sel)
        throw new SQLException("Error in "+sel, e)
    }
  }
  def execute(sel: String, args: List[PrimitiveValue]): ResultSet = 
  {
    //println(""+sel+" <- "+args)
    try {
      if(conn == null) {
        throw new SQLException("Trying to use unopened connection!")
      }
      val stmt = conn.prepareStatement(sel)
      setArgs(stmt, args)
      stmt.executeQuery()
    } catch { 
      case e: SQLException => println(e.toString+"during\n"+sel+" <- "+args)
        throw new SQLException("Error", e)
    }
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

  def update(upd: String, args: List[PrimitiveValue]): Unit =
  {
    if(conn == null) {
      throw new SQLException("Trying to use unopened connection!")
    }
    val stmt = conn.prepareStatement(upd);
    setArgs(stmt, args)
    var i: Int = 0
    stmt.execute()
    stmt.close()
  }

  def setArgs(stmt: PreparedStatement, args: List[PrimitiveValue]): Unit =
  {
    args.zipWithIndex.foreach(a => {
      val i = a._2+1
      a._1 match {
        case p:StringPrimitive   => stmt.setString(i, p.v)
        case p:IntPrimitive      => stmt.setLong(i, p.v)
        case p:FloatPrimitive    => stmt.setDouble(i, p.v)
        case b:BlobPrimitive     => stmt.setBytes(i, b.v)
        case _:NullPrimitive     => stmt.setNull(i, Types.VARCHAR)
      }
    })
  }
  
  def getTableSchema(table: String): Option[List[(String, Type.T)]] =
  {
    if(conn == null) {
      throw new SQLException("Trying to use unopened connection!")
    }

    tableSchemas.get(table) match {
      case x: Some[_] => x
      case None =>
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

        tableSchemas += table -> ret
        Some(ret)

    }
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

  def specializeQuery(q: Operator): Operator = {
    backend match {
      case "sqlite" => SpecializeForSQLite(q)
      case "oracle" => q
    }
  }

  
}