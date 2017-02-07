package mimir.sql;

import java.sql._

import mimir.Database
import mimir.Methods
import mimir.algebra._
import mimir.util.JDBCUtils
import mimir.sql.sqlite._
import java.util.Properties
import org.gprom.jdbc.driver.GProMDriverProperties
import org.gprom.jdbc.driver.GProMDriver

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import mimir.gprom.MimirGProMMetadataPlugin
import org.gprom.jdbc.driver.GProMConnection
import org.gprom.jdbc.jna.GProMWrapper

class GProMBackend(backend: String, filename: String, var gpromLogLevel : Int) extends Backend
{
  var conn: Connection = null
  var openConnections = 0
  var inliningAvailable = false
  //var gpromMetadataPlugin : MimirGProMMetadataPlugin = null
  
  def driver() = backend

  val tableSchemas: scala.collection.mutable.Map[String, Seq[(String, Type)]] = mutable.Map()
  
  def setGProMLogLevel(level : Int) : Unit = {
    gpromLogLevel = level
    if(gpromLogLevel == -1)
      GProMWrapper.inst.setSilenceLogger(true)
    else
      GProMWrapper.inst.setSilenceLogger(false)
    if(conn != null){
      conn.asInstanceOf[GProMConnection].getW().setLogLevel(level)
    }
  }
  
  def open() = {
    this.synchronized({
      assert(openConnections >= 0)
      if (openConnections == 0) {
        conn = backend match {
          case "sqlite" =>
            Class.forName("org.gprom.jdbc.driver.GProMDriver")
            Class.forName("org.sqlite.JDBC")
            val path = java.nio.file.Paths.get(filename).toString
            val info = new Properties()
            if(gpromLogLevel == -1){
              GProMWrapper.inst.setSilenceLogger(true)
            }
			      info.setProperty(GProMDriverProperties.JDBC_METADATA_LOOKUP, "TRUE")
            info.setProperty("plugin.analyzer","sqlite")
            info.setProperty("plugin.translator","sqlite")
            info.setProperty("log.active","false")
            var c = java.sql.DriverManager.getConnection("jdbc:gprom:sqlite:" + path, info)
            c.asInstanceOf[GProMConnection].getW().setLogLevel(gpromLogLevel)
            //gpromMetadataPlugin = new MimirGProMMetadataPlugin()
            SQLiteCompat.registerFunctions( c.unwrap[org.sqlite.SQLiteConnection]( classOf[org.sqlite.SQLiteConnection] ) )
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

  def enableInlining(db: Database): Unit =
  {
    backend match {
      case "sqlite" => 
        sqlite.VGTermFunctions.register(db, conn.unwrap[org.sqlite.SQLiteConnection]( classOf[org.sqlite.SQLiteConnection]))
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
    //println("EX1: " +sel)
    this.synchronized({
      try {
        if(conn == null) {
          throw new SQLException("Trying to use unopened connection!")
        }
        val stmt = conn.unwrap[org.sqlite.SQLiteConnection]( classOf[org.sqlite.SQLiteConnection]).createStatement()
        var repSel = sel
        if(!sel.endsWith(";")) 
          repSel = repSel + ";"
        val ret = stmt.executeQuery(repSel)
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
    //println("EX2: " +sel)
    this.synchronized({
      try {
        if(conn == null) {
          throw new SQLException("Trying to use unopened connection!")
        }
        //val stmt = conn.prepareStatement(sel)
        //setArgs(stmt, args)
        //stmt.executeQuery()
        var repSel = sel;
        for ( x <- args ) {
           repSel = repSel.replaceFirst("\\?", "'"+x.asString + "'")
        }
        val stmt = conn.unwrap[org.sqlite.SQLiteConnection]( classOf[org.sqlite.SQLiteConnection]).createStatement()
        if(!repSel.endsWith(";")) 
          repSel = repSel + ";"
        //println("UPS2: " +repSel)
        val ret = stmt.executeQuery(repSel)
        stmt.closeOnCompletion()
        ret
      } catch { 
        case e: SQLException => println(e.toString+"during\n"+sel+" <- "+args)
          throw new SQLException("Error", e)
          null
      }
    })
    
  }
  
  def update(upd: String): Unit =
  {
    //println("UP1: " +upd)
    this.synchronized({
      if(conn == null) {
        throw new SQLException("Trying to use unopened connection!")
      }
      var repSel = upd
      if(!repSel.endsWith(";")) 
          repSel = repSel + ";"
        
      val stmt = conn.unwrap[org.sqlite.SQLiteConnection]( classOf[org.sqlite.SQLiteConnection]).createStatement()
      stmt.executeUpdate(repSel)
      stmt.close()
    })
  }

  def update(upd: TraversableOnce[String]): Unit =
  {
    this.synchronized({
      if(conn == null) {
        throw new SQLException("Trying to use unopened connection!")
      }
      val stmt = conn.unwrap[org.sqlite.SQLiteConnection]( classOf[org.sqlite.SQLiteConnection]).createStatement()
      upd.foreach( u => {
          //println("UP2: " +u)
          var repSel = u
          if(!repSel.endsWith(";")) 
            repSel = repSel + ";"
          stmt.addBatch(repSel) 
        }
      )
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
      var repSel = upd;
      for ( x <- args ) {
         repSel = repSel.replaceFirst("\\?", "'"+x.asString + "'")
      }
      if(!repSel.endsWith(";")) 
            repSel = repSel + ";"
      val stmt = conn.unwrap[org.sqlite.SQLiteConnection]( classOf[org.sqlite.SQLiteConnection]).createStatement()
      //println("UP3: " + repSel)
      stmt.execute(repSel)
      stmt.close()
    })
  }

  def fastUpdateBatch(upd: String, argsList: TraversableOnce[Seq[PrimitiveValue]]): Unit =
  {
    this.synchronized({
      if(conn == null) {
        throw new SQLException("Trying to use unopened connection!")
      }
      /*if(backend.equals("sqlite")){
        // Borrowing some advice from :
        // http://blog.quibb.org/2010/08/fast-bulk-inserts-into-sqlite/
        update("PRAGMA synchronous=OFF")
        update("PRAGMA journal_mode=MEMORY")
        update("PRAGMA temp_store=MEMORY")
      }*/
      println("turned off fast bulk insert changes for GProM testing..."); 
      conn.unwrap[org.sqlite.SQLiteConnection]( classOf[org.sqlite.SQLiteConnection]).setAutoCommit(false)
      try {
        argsList.foreach( (args) => {
          var repSel = upd;
          for ( x <- args ) {
             repSel = repSel.replaceFirst("\\?", "'"+x.asString + "'")
          }
          if(!repSel.endsWith(";")) 
            repSel = repSel + ";"
          val stmt = conn.unwrap[org.sqlite.SQLiteConnection]( classOf[org.sqlite.SQLiteConnection]).createStatement()
          //println("UP4: " +repSel)
          stmt.execute(repSel)
          stmt.close()
          
        })
        
      } finally {
        conn.unwrap[org.sqlite.SQLiteConnection]( classOf[org.sqlite.SQLiteConnection]).commit()
        conn.unwrap[org.sqlite.SQLiteConnection]( classOf[org.sqlite.SQLiteConnection]).setAutoCommit(true)
        /*if(backend.equals("sqlite")){
          update("PRAGMA synchronous=ON")
          update("PRAGMA journal_mode=DELETE")
          update("PRAGMA temp_store=DEFAULT")
        }*/
      }
    })
  }
  
  def getTableSchema(table: String): Option[Seq[(String, Type)]] =
  {
    this.synchronized({
      if(conn == null) {
        throw new SQLException("Trying to use unopened connection!")
      }

      tableSchemas.get(table) match {
        case x: Some[_] => x
        case None =>
          val tables = this.getAllTables().map{(x) => x.toUpperCase}
          if(!tables.contains(table.toUpperCase)) return None

          val cols: Option[List[(String, Type)]] = backend match {
            case "sqlite" => {
              // SQLite doesn't recognize anything more than the simplest possible types.
              // Type information is persisted but not interpreted, so conn.getMetaData() 
              // is useless for getting schema information.  Instead, we need to use a
              // SQLite-specific PRAGMA operation.
              SQLiteCompat.getTableSchema(conn.unwrap[org.sqlite.SQLiteConnection]( classOf[org.sqlite.SQLiteConnection] ), table)
            }
            case "oracle" => 
              val columnRet = conn.unwrap[org.sqlite.SQLiteConnection]( classOf[org.sqlite.SQLiteConnection]).getMetaData().getColumns(null, "ARINDAMN", table, "%")  // TODO Generalize
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
          }
          
          cols match { case None => (); case Some(s) => tableSchemas += table -> s }
          cols
      }
    })
  }

  def getAllTables(): Seq[String] = {
    this.synchronized({
      if(conn == null) {
        throw new SQLException("Trying to use unopened connection!")
      }

      val metadata = conn.unwrap[org.sqlite.SQLiteConnection]( classOf[org.sqlite.SQLiteConnection]).getMetaData()
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
    })
  }

  def canHandleVGTerms(): Boolean = inliningAvailable

  def specializeQuery(q: Operator): Operator = {
    backend match {
      case "sqlite" if inliningAvailable => 
        VGTermFunctions.specialize(SpecializeForSQLite(q))
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