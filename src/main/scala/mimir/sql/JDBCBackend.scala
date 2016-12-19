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

  val tableSchemas: scala.collection.mutable.Map[String, Seq[(String, Type)]] = mutable.Map()

  def open() = {
    this.synchronized({
      assert(openConnections >= 0)
      if (openConnections == 0) {
        conn = backend match {
          case "sqlite" =>
            Class.forName("org.sqlite.JDBC")
            val path = java.nio.file.Paths.get(filename).toString
            var c = java.sql.DriverManager.getConnection("jdbc:sqlite:" + path)
            SQLiteCompat.registerFunctions(c)
            c

          case "oracle" =>
            Methods.getConn()

          case "mysql" =>
            val url = "jdbc:mysql://localhost/"+filename // filename here is the MySQL database name
            val username = "root"
            val password = "password"
            Class.forName("com.mysql.jdbc.Driver")
            var c:Connection = DriverManager.getConnection(url, username, password)
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
  def execute(sel: String, args: Seq[PrimitiveValue]): ResultSet = 
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

  def update(upd: TraversableOnce[String]): Unit =
  {
    if(conn == null) {
      throw new SQLException("Trying to use unopened connection!")
    }
    val stmt = conn.createStatement()
    upd.foreach( u => stmt.addBatch(u) )
    stmt.executeBatch()
    stmt.close()
  }

  def update(upd: String, args: Seq[PrimitiveValue]): Unit =
  {
    if(conn == null) {
      throw new SQLException("Trying to use unopened connection!")
    }
    val stmt = conn.prepareStatement(upd);
    setArgs(stmt, args)
    stmt.execute()
    stmt.close()
  }
  
  def getTableSchema(table: String): Option[Seq[(String, Type)]] =
  {
    if(conn == null) {
      throw new SQLException("Trying to use unopened connection!")
    }

    tableSchemas.get(table) match {
      case x: Some[_] => x
      case None =>
        val tables = this.getAllTables().map{(x) => x.toUpperCase}
        if(!tables.contains(table.toUpperCase)) return None

        val cols: Option[List[(String, Type)]] = backend match {
          case "sqlite" | "sqlite-inline" | "sqlite-bundles" => {
            // SQLite doesn't recognize anything more than the simplest possible types.
            // Type information is persisted but not interpreted, so conn.getMetaData() 
            // is useless for getting schema information.  Instead, we need to use a
            // SQLite-specific PRAGMA operation.
            SQLiteCompat.getTableSchema(conn, table)
          }
          case "oracle" => 
            val columnRet = conn.getMetaData().getColumns(null, "ARINDAMN", table, "%")  // TODO Generalize
            var ret = List[(String, Type)]()
            while(columnRet.isBeforeFirst()){ columnRet.next(); }
            while(!columnRet.isAfterLast()){
              ret = ret ++ List((
                columnRet.getString("COLUMN_NAME").toUpperCase,
                JDBCUtils.convertSqlType(columnRet.getInt("DATA_TYPE"))
                ))
              columnRet.next()
            }
            columnRet.close()
            Some(ret)

          case "mysql" => {
            val stmt = conn.createStatement()
            val ret = stmt.executeQuery(s"DESC $table")
            val result = JDBCUtils.extractAllRows(ret).map( (x) => {
              val name = x(0).asString.toUpperCase.trim
              val rawType = Type.fromString(x(1).asString.replaceAll("[^a-zA-Z]",""))
              val nullType = x(2).asString.trim
              val key = x(3).asString.trim
              val extra = x(5).asString.trim

              (name, rawType)
            })

            if(result.hasNext){ Some(result.toList) } else { None }
          }
        }

        cols match { case None => (); case Some(s) => tableSchemas += table -> s }
        cols
    }
  }

  def getAllTables(): Seq[String] = {
    if(conn == null) {
      throw new SQLException("Trying to use unopened connection!")
    }

    val metadata = conn.getMetaData()
    val tables = backend match {
      case "sqlite" => metadata.getTables(null, null, "%", null)
      case "mysql" => metadata.getTables(null, null, "%", null);
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

  def setArgs(stmt: PreparedStatement, args: Seq[PrimitiveValue]): Unit =
  {
    args.zipWithIndex.foreach(a => {
      val i = a._2+1
      a._1 match {
        case p:StringPrimitive   => stmt.setString(i, p.v)
        case p:IntPrimitive      => stmt.setLong(i, p.v)
        case p:FloatPrimitive    => stmt.setDouble(i, p.v)
        case _:NullPrimitive     => stmt.setNull(i, Types.VARCHAR)
        case d:DatePrimitive     => stmt.setDate(i, JDBCUtils.convertDate(d))
        case r:RowIdPrimitive    => stmt.setString(i,r.v)
      }
    })
  }
}