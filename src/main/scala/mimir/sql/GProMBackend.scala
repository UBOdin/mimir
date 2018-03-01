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
import com.sun.jna.Native
import org.apache.spark.sql.DataFrame

class GProMBackend(backend: String, filename: String, var gpromLogLevel : Int) 
  extends RABackend
{
  var conn: Connection = null
  var unwrappedConn: org.sqlite.SQLiteConnection = null
  var openConnections = 0
  var inliningAvailable = false
  //var gpromMetadataPlugin : MimirGProMMetadataPlugin = null
  var metadataLookupPlugin : GProMMedadataLookup = null
  
  def driver() = backend

  val tableSchemas: scala.collection.mutable.Map[String, Seq[(String, Type)]] = mutable.Map()

  def setGProMLogLevel(level : Int) : Unit = {
    GProMWrapper.inst.setLogLevel(level)
  }

  def open() = {
    this.synchronized({
      System.setProperty("jna.protected","true")
      Native.setProtected(true)
      println(s"GProM Library running in protected mode: ${Native.isProtected()}")
      assert(openConnections >= 0)
      if (openConnections == 0) {
        conn = backend match {
          case "sqlite" =>
            GProMWrapper.inst.setInitialLogLevel(gpromLogLevel)
			      Class.forName("org.gprom.jdbc.driver.GProMDriver")
            Class.forName("org.sqlite.JDBC")
            val path = java.nio.file.Paths.get(filename).toString
            val info = new Properties()
            info.setProperty(GProMDriverProperties.JDBC_METADATA_LOOKUP, "TRUE")
            info.setProperty("plugin.analyzer","sqlite")
            info.setProperty("plugin.translator","sqlite")
            info.setProperty("log.active","false")
            var c = java.sql.DriverManager.getConnection("jdbc:gprom:sqlite:" + path, info)
            //GProMWrapper.inst.setLogLevel(gpromLogLevel)
            //gpromMetadataPlugin = new MimirGProMMetadataPlugin()
            unwrappedConn = c.unwrap[org.sqlite.SQLiteConnection]( classOf[org.sqlite.SQLiteConnection])
            SQLiteCompat.registerFunctions( unwrappedConn )
            metadataLookupPlugin = new GProMMedadataLookup(c)
            GProMWrapper.inst.setupPlugins(c, metadataLookupPlugin.getPlugin) 
            //GProMWrapper.inst.setBoolOption("pi_cs_use_composable", true)
           // GProMWrapper.inst.setBoolOption("pi_cs_rewrite_agg_window", false)
           // GProMWrapper.inst.setBoolOption("cost_based_optimizer", false)
           // GProMWrapper.inst.setBoolOption("optimization.remove_unnecessary_window_operators", true)
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
        sqlite.VGTermFunctions.register(db, unwrappedConn)
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
  
  def execute(compiledOp: Operator): DataFrame = ???
  def execute(compiledOp: Operator, args: Seq[PrimitiveValue]): DataFrame = ???
  def createTable(tableName: String,oper: mimir.algebra.Operator): Unit = ???
  def readDataSource(name: String,format: String,options: Map[String,String],schema: Option[Seq[(String, mimir.algebra.Type)]],load: Option[String]): Unit = ???

  def execute(sel: String): ResultSet =
  {
    //println("EX1: " +sel)
    this.synchronized({
      try {
        if(conn == null) {
          throw new SQLException("Trying to use unopened connection!")
        }
        val stmt = unwrappedConn.createStatement()
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
        val stmt = unwrappedConn.createStatement()
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

      val stmt = unwrappedConn.createStatement()
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
      val stmt = unwrappedConn.createStatement()
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
      val stmt = unwrappedConn.createStatement()
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
      unwrappedConn.setAutoCommit(false)
      try {
        argsList.foreach( (args) => {
          var repSel = upd;
          for ( x <- args ) {
             val paramStr = x match {
               case NullPrimitive() => "NULL"
               case _ => s"'${x.asString}'"
             }
             repSel = repSel.replaceFirst("\\?", paramStr)
          }
          if(!repSel.endsWith(";"))
            repSel = repSel + ";"
          val stmt = unwrappedConn.createStatement()
          //println("UP4: " +repSel)
          stmt.execute(repSel)
          stmt.close()

        })

      } finally {
        unwrappedConn.commit()
        unwrappedConn.setAutoCommit(true)
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

          val cols: Option[Seq[(String, Type)]] = backend match {
            case "sqlite" => {
              // SQLite doesn't recognize anything more than the simplest possible types.
              // Type information is persisted but not interpreted, so conn.getMetaData()
              // is useless for getting schema information.  Instead, we need to use a
              // SQLite-specific PRAGMA operation.
              SQLiteCompat.getTableSchema(unwrappedConn, table)
            }
            case "oracle" =>
              val columnRet = unwrappedConn.getMetaData().getColumns(null, "ARINDAMN", table, "%")  // TODO Generalize
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

      val metadata = unwrappedConn.getMetaData()
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

  def specializeQuery(q: Operator, db: Database): Operator = {
    backend match {
      case "sqlite" if inliningAvailable =>
        VGTermFunctions.specialize(SpecializeForSQLite(q, db))
      case "sqlite" => SpecializeForSQLite(q, db)
      case "oracle" => q
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
        Project(
          Seq(
            ProjectArg("TABLE_NAME", Var("NAME"))
          ),
          Select(
            ExpressionUtils.makeInTest(Var("TYPE"), Seq(StringPrimitive("table"), StringPrimitive("view"))),
            Table("SQLITE_MASTER", "SQLITE_MASTER", Seq(("NAME", TString()), ("TYPE", TString())), Seq())
          )
        )

      case "oracle" => ???
    }
  }
  def listAttrsQuery: Operator = 
  {
    backend match {
      case "sqlite" => {
        //logger.warn("SQLITE has no programatic way to access attributes in SQL")
        HardTable(Seq(
          ("TABLE_NAME", TString()), 
          ("ATTR_NAME", TString()),
          ("ATTR_TYPE", TString()),
          ("IS_KEY", TBool())
        ),Seq());
      }

      case "oracle" => ???
    }
  }

  def selectInto(table: String, query: String){
    backend match {
      case "sqlite" => 
        update(s"CREATE TABLE $table AS $query")
    }
  }
  
  def dateType: mimir.algebra.Type = TDate()
  def invalidateCache(): Unit = {}
  def rowIdType: mimir.algebra.Type = TRowId()
}
