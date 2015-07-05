package mimir

import java.io.{File, StringReader}

import mimir.exec.ResultSetIterator
import mimir.parser.MimirJSqlParser
import mimir.sql.{CreateLens, Explain, JDBCBackend}
import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.select.Select

import scala.collection.mutable.ListBuffer

/**
 * Created by arindam on 6/29/15.
 */

class WebAPI {

  var conf: MimirConfig = null
  var db: Database = null
  var dbName: String = null

  /* Initialize the configuration and database */
  def configure(args: Array[String]): Unit = {
    conf = new MimirConfig(args)

    // Set up the database connection(s)
    dbName = conf.dbname()
    val backend = conf.backend() match {
      case "oracle" => new JDBCBackend(Mimir.connectOracle())
      case "sqlite" => new JDBCBackend(Mimir.connectSqlite(dbName))
      case x => {
        println("Unsupported backend: "+x)
        sys.exit(-1)
      }
    }

    db = new Database(backend)

    if(conf.initDB()){
      db.initializeDBForMimir()
    } else if(conf.loadTable.get != None){
      Mimir.handleLoadTable(db, conf.loadTable(), conf.loadTable()+".csv")
    }
  }

  def handleStatement(query: String): WebQueryResult = {
    if(db == null) {
      new WebQueryResult(false, "Database is not configured properly", null)
    }

    val source = new StringReader(query)
    val parser = new MimirJSqlParser(source)

    try {
      val stmt: Statement = parser.Statement();
      if(stmt.isInstanceOf[Select]){
        handleSelect(stmt.asInstanceOf[Select])
      } else if(stmt.isInstanceOf[CreateLens]) {
        db.createLens(stmt.asInstanceOf[CreateLens])
        new WebQueryResult(false, "Lens created successfully.", null)
      } else if(stmt.isInstanceOf[Explain]) {
        handleExplain(stmt.asInstanceOf[Explain])
      } else {
        db.update(stmt.toString())
        new WebQueryResult(false, "Database updated.", null)
      }

    } catch {
      case e: Throwable => {
        e.printStackTrace()
        new WebQueryResult(false, "Command Ignored\n\n"+e.getMessage, null)
      }
    }

  }

  private def handleSelect(sel: Select): WebQueryResult = {
    val raw = db.convert(sel)
    val results = db.query(raw)

    results.open()
    val wIter = db.webDump(results)
    results.close()

    new WebQueryResult(true, null, wIter)
  }

  private def handleExplain(explain: Explain): WebQueryResult = {
    val raw = db.convert(explain.getSelectBody())._1;

    val res = "------ Raw Query ------\n"+
      raw.toString()+"\n"+
      "--- Optimized Query ---\n"+
      db.optimize(raw).toString

    new WebQueryResult(false, res, null)
  }

  def getAllTables(): List[String] = {
    val res = db.backend.getAllTables()
    val iter = new ResultSetIterator(res)
    val tableNames = new ListBuffer[String]()

    iter.open()
    while(iter.getNext()) {
      val name = iter(2).asString
      if(!name.equalsIgnoreCase("MIMIR_LENSES")) tableNames.append(name)
    }
    iter.close()

    tableNames.toList
  }

  def getAllLenses(): List[String] = {
    val res = db.backend.execute(
      """
        SELECT *
        FROM MIMIR_LENSES
      """)

    val iter = new ResultSetIterator(res)
    val lensNames = new ListBuffer[String]()

    iter.open()
    while(iter.getNext()) {
      lensNames.append(iter(0).asString)
    }
    iter.close()

    lensNames.toList
  }

  def getAllDBs(): Array[String] = {
    val curDir = new File(".")
    curDir.listFiles().filter( f => f.isFile && f.getName.endsWith(".db")).map(x => x.getName)
  }

  def close(): Unit = {
    db.backend.close()
  }
}



class WebQueryResult(sResult: Boolean,
                  rString: String,
                  wIterator: WebIterator) {

  val selectResult = sResult
  val resultString = rString
  val webIterator = wIterator

}

class WebIterator(h: List[String],
                  d: List[(List[String], Boolean)],
                  mR: Boolean) {

  val header = h
  val data = d
  val missingRows = mR

}
