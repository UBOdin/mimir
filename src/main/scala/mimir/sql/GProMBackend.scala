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
import mimir.ml.spark.SparkML

class GProMBackend(backend: String, filename: String, var gpromLogLevel : Int) 
  extends RABackend
{
  var conn: Connection = null
  var unwrappedConn: org.sqlite.SQLiteConnection = null
  var openConnections = 0
  var inliningAvailable = false
  //var gpromMetadataPlugin : MimirGProMMetadataPlugin = null
  var metadataLookupPlugin : GProMMedadataLookup = null
  var sparkBackend:SparkBackend = null
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
            sparkBackend = new SparkBackend()
            sparkBackend.open()
            
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
    sparkBackend.close()
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
  
  def execute(compiledOp: Operator): DataFrame = sparkBackend.execute(compiledOp)
  def materializeView(name:String): Unit = sparkBackend.materializeView(name)
  def createTable(tableName: String,oper: mimir.algebra.Operator): Unit = sparkBackend.createTable(tableName, oper)
  def readDataSource(name: String,format: String,options: Map[String,String],schema: Option[Seq[(String, mimir.algebra.Type)]],load: Option[String]): Unit = sparkBackend.readDataSource(name, format, options, schema, load)
  def getTableSchema(table: String): Option[Seq[(String, Type)]] = sparkBackend.getTableSchema(table)
  
  
  def getAllTables(): Seq[String] = sparkBackend.getAllTables()
  def invalidateCache() = sparkBackend.invalidateCache()

 
  def canHandleVGTerms: Boolean = sparkBackend.canHandleVGTerms
  def rowIdType: Type = sparkBackend.rowIdType
  def dateType: Type = sparkBackend.dateType
  def specializeQuery(q: Operator, db: Database): Operator = sparkBackend.specializeQuery(q, db)

  def listTablesQuery: Operator = sparkBackend.listTablesQuery
  
  def listAttrsQuery: Operator = sparkBackend.listAttrsQuery
  
}
