package mimir.backend;

import java.sql._

import mimir.Database
import mimir.algebra._
import mimir.util.JDBCUtils
import mimir.backend.sqlite._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import com.typesafe.scalalogging.slf4j.LazyLogging

class JDBCMetadataBackend(val backend: String, val filename: String)
  extends MetadataBackend
  with LazyLogging
  with InlinableBackend
{
  var conn: Connection = null
  var openConnections = 0
  var inliningAvailable = false;

  def driver() = backend

  val tableSchemas: scala.collection.mutable.Map[ID, Seq[(ID, Type)]] = mutable.Map()

  def open() = 
  {
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

          case x =>
            println("Unsupported backend! Exiting..."); System.exit(-1); null
        }
      }

      assert(conn != null)
      openConnections = openConnections + 1
    })
  }

  def invalidateCache() = 
    tableSchemas.clear()

  def enableInlining(db: Database): Unit =
  {
    backend match {
      case "sqlite" => 
        sqlite.VGTermFunctions.register(db, conn)
        inliningAvailable = true
    }
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
    this.synchronized({
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
    })
  }
  def execute(sel: String, args: Seq[PrimitiveValue]): ResultSet = 
  {
    this.synchronized({
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
    })
  }
  
  def update(upd: String): Unit =
  {
    this.synchronized({
      if(conn == null) {
        throw new SQLException("Trying to use unopened connection!")
      }
      val stmt = conn.createStatement()
      stmt.executeUpdate(upd)
      stmt.close()
    })
  }

  def update(upd: TraversableOnce[String]): Unit =
  {
    this.synchronized({
      if(conn == null) {
        throw new SQLException("Trying to use unopened connection!")
      }
      val stmt = conn.createStatement()
      upd.foreach( u => stmt.addBatch(u) )
      stmt.executeBatch()
      stmt.close()
    })
  }

  def update(upd: String, args: Seq[PrimitiveValue]): Unit =
  {
    this.synchronized({
      if(conn == null) {
        throw new SQLException("Trying to use unopened connection!")
      }
      val stmt = conn.prepareStatement(upd);
      setArgs(stmt, args)
      stmt.execute()
      stmt.close()
    })
  }

  def fastUpdateBatch(upd: String, argsList: TraversableOnce[Seq[PrimitiveValue]]): Unit =
  {
    this.synchronized({
      if(conn == null) {
        throw new SQLException("Trying to use unopened connection!")
      }
      if(backend.equals("sqlite")){
        // Borrowing some advice from :
        // http://blog.quibb.org/2010/08/fast-bulk-inserts-into-sqlite/
        update("PRAGMA synchronous=OFF")
        update("PRAGMA journal_mode=MEMORY")
        update("PRAGMA temp_store=MEMORY")
      }
      conn.setAutoCommit(false)
      try {
        val stmt = conn.prepareStatement(upd);
        var idx = 0
        argsList.foreach( (args) => {
          idx += 1
          setArgs(stmt, args)
          stmt.execute()
          if(idx % 500000 == 0){ conn.commit() }
        })
        stmt.close()
      } finally {
        conn.commit()
        conn.setAutoCommit(true)
        if(backend.equals("sqlite")){
          update("PRAGMA synchronous=ON")
          update("PRAGMA journal_mode=DELETE")
          update("PRAGMA temp_store=DEFAULT")
        }
      }
    })
  }
  def selectInto(table: ID, query: String): Unit =
  {
    backend match {
      case "sqlite" => 
        update(s"CREATE TABLE $table AS $query")
    }
  }
  
  def getTableSchema(table: ID): Option[Seq[(ID, Type)]] =
  {
    this.synchronized({
      if(conn == null) {
        throw new SQLException("Trying to use unopened connection!")
      }

      tableSchemas.get(table) match {
        case x: Some[_] => x
        case None =>
          val tables = this.getAllTables()
          if(!tables.contains(table)) return None

          val cols: Option[Seq[(ID, Type)]] = backend match {
            case "sqlite" => {
              // SQLite doesn't recognize anything more than the simplest possible types.
              // Type information is persisted but not interpreted, so conn.getMetaData() 
              // is useless for getting schema information.  Instead, we need to use a
              // SQLite-specific PRAGMA operation.
              SQLiteCompat.getTableSchema(conn, table)
            }
          }
          
          cols match { case None => (); case Some(s) => tableSchemas += table -> s }
          cols
      }
    })
  }

  def getAllTables(): Seq[ID] = {
    this.synchronized({
      if(conn == null) {
        throw new SQLException("Trying to use unopened connection!")
      }

      val tableNames = new ListBuffer[ID]()
      val metadata = conn.getMetaData()
      val tables = backend match {
        case "sqlite" => {
          tableNames.append(ID("SQLITE_MASTER"))
          metadata.getTables(null, null, "%", null)
        }
      }


      while(tables.next()) {
        tableNames.append(ID(tables.getString("TABLE_NAME")))
      }

      tables.close()
      tableNames.toList
    })
  }

  def canHandleVGTerms: Boolean = inliningAvailable
  def rowIdType: Type = 
    backend match {
      case "sqlite" => TInt()
      case _ => TString()
    }
  def dateType: Type =
    backend match {
      case "sqlite" => TString()
      case _ => TDate()
    }

  def specializeQuery(q: Operator, db: Database): Operator = {
    backend match {
      case "sqlite" if inliningAvailable => 
        VGTermFunctions.specialize(SpecializeForSQLite(q, db))
      case "sqlite" => SpecializeForSQLite(q, db)
    }
  }

  def setArgs(stmt: PreparedStatement, args: Seq[PrimitiveValue]): Unit =
  {
    args.zipWithIndex.foreach(a => {
      val i = a._2+1
      a._1 match {
        case p:StringPrimitive    => stmt.setString(i, p.v)
        case p:IntPrimitive       => stmt.setLong(i, p.v)
        case p:FloatPrimitive     => stmt.setDouble(i, p.v)
        case _:NullPrimitive      => stmt.setNull(i, Types.VARCHAR)
        case d:DatePrimitive      => 
          backend match {
            case "sqlite" => 
              stmt.setString(i, d.asString )
            case _ =>
              stmt.setDate(i, JDBCUtils.convertDate(d))
          }
        case t:TimestampPrimitive      => 
          backend match {
            case "sqlite" => 
              stmt.setString(i, t.asString )
            case _ =>
              stmt.setTimestamp(i, JDBCUtils.convertTimestamp(t))
          }
        case t:IntervalPrimitive  => 
          backend match {
            case _ => throw new SQLException(s"$backend does not support intervals in prepared statements")
          }
        case r:RowIdPrimitive     => stmt.setString(i,r.v)
        case t:TypePrimitive      => stmt.setString(i, t.t.toString) 
        case BoolPrimitive(true)  => stmt.setInt(i, 1)
        case BoolPrimitive(false) => stmt.setInt(i, 0)
      }
    })
  }

  def listTablesQuery: Operator = 
  {
    backend match {
      case "sqlite" => 
        val tableDirectory = 
          Table(
            ID("SQLITE_MASTER"), 
            ID("SQLITE_MASTER"), 
            Seq(
              ID("NAME") -> TString(),
              ID("TYPE") -> TString()
            ), 
            Seq()
          )
        val t = Var(ID("TYPE"))

        tableDirectory
          .filter { t.eq("table").or { t.eq("view") } }
          .map { "TABLE_NAME" -> Var(ID("NAME")) }
    }
  }
  def listAttrsQuery: Operator = 
  {
    backend match {
      case "sqlite" => {
        HardTable(Seq(
            ID("TABLE_NAME") -> TString(), 
            ID("ATTR_NAME") -> TString(),
            ID("ATTR_TYPE") -> TString(),
            ID("IS_KEY") -> TBool()
          ),
          getAllTables().flatMap { table =>
            getTableSchema(table).get.map { case (col, t) =>
              Seq(
                StringPrimitive(table.id),
                StringPrimitive(col.id),
                TypePrimitive(t),
                BoolPrimitive(false)
              )
            }
          }
        )
      }
    }
  }
  
  def insertAndReturnKey(insertSql:String,  args: Seq[PrimitiveValue]) : Long = {
    try {
        this.synchronized({
          if(conn == null) 
            throw new SQLException("Trying to use unopened connection!")
          val stmt = conn.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS);
          setArgs(stmt, args)                             
          val affectedRows = stmt.executeUpdate();
          if (affectedRows == 0) 
              throw new SQLException("Insert did not generate any rows.");
          val generatedKey = try{
            val generatedKeys = stmt.getGeneratedKeys() 
            if (generatedKeys.next())
                generatedKeys.getLong(1)
            else 
                throw new SQLException("No Key Returned.");
          } catch { 
            case e: SQLException => println(e.toString+"during\n"+insertSql+" <- "+args)
            throw new SQLException("Error", e)
          }
          stmt.close()
          generatedKey
        })
    } catch { 
        case e: SQLException => println(e.toString+"during\n"+insertSql+" <- "+args)
          throw new SQLException("Error", e)
      }
  }
  
}