package mimir

import java.io.{File, StringReader}
import java.sql.SQLException

import mimir.algebra._
import mimir.ctables.{CTPercolator, CTables, VGTerm, Explanation, Reason}
import mimir.exec.ResultSetIterator
import mimir.parser.MimirJSqlParser
import mimir.sql.{CreateLens, Explain, JDBCBackend}
import mimir.web._
import mimir.util.JSONBuilder
import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.insert.Insert
import net.sf.jsqlparser.statement.select.Select
import net.sf.jsqlparser.statement.update.Update
import net.sf.jsqlparser.util.deparser.{InsertDeParser, ExpressionDeParser, SelectDeParser, UpdateDeParser}

import scala.collection.mutable.ListBuffer

/* Web-facing API */

class WebAPI(dbName: String = "tpch.db", backend: String = "sqlite") {


  val db = new Database(dbName, new JDBCBackend(backend, dbName))

  def openBackendConnection(): Unit = {
    db.backend.open()
  }

  def getCurrentDB = dbName

  def initializeDBForMimir() = {
    db.initializeDBForMimir()
  }

  def handleLoadTable(filename: String) = {
    db.loadTable(filename.replace(".csv", ""), filename)
  }

  def handleStatement(query: String): (WebResult, String) = {
    if(db == null) {
      new WebStringResult("Database is not configured properly")
    }

    val source = new StringReader(query)
    val parser = new MimirJSqlParser(source)

    var res: WebResult = null
    var lastStatement = ""
    var done = false

    while(!done) {
      try {
        val stmt: Statement = parser.Statement()

        if(stmt == null) done = true
        else {
          lastStatement = stmt.toString
          res = stmt match {
            case s: Select => handleSelect(s.asInstanceOf[Select])
            case s: CreateLens =>
              db.createLens(s.asInstanceOf[CreateLens])
              new WebStringResult("Lens created successfully.")

            case s: Explain => handleExplain(s.asInstanceOf[Explain])
            case s: Insert =>
              val buffer = new StringBuffer()
              val insDeParser = new InsertDeParser(new ExpressionDeParser(new SelectDeParser(), buffer), new SelectDeParser(), buffer)
              insDeParser.deParse(s)
              db.update(insDeParser.getBuffer.toString)
              new WebStringResult("Database updated.")
            case s: Update =>
              val buffer = new StringBuffer()
              val updDeParser = new UpdateDeParser(new ExpressionDeParser(new SelectDeParser(), buffer), buffer)
              updDeParser.deParse(s)
              db.update(updDeParser.getBuffer.toString)
              new WebStringResult("Database updated.")
          }
        }
      } catch {
        case e: Throwable => {
          done = true
          e.printStackTrace()
          res = new WebErrorResult(e.getMessage)
        }
      }
    }

    (res, lastStatement)
  }

  private def handleSelect(sel: Select): WebQueryResult = {
    val start = System.nanoTime()
    val raw = db.convert(sel)
    val rawT = System.nanoTime()
    val results = db.query(raw)
    val resultsT = System.nanoTime()

    println("Convert time: "+((rawT-start)/(1000*1000))+"ms")
    println("Compile time: "+((resultsT-rawT)/(1000*1000))+"ms")

    results.open()
    val wIter: WebIterator = db.generateWebIterator(results)
    try{
      wIter.queryFlow = convertToTree(raw)
    } catch {
      case e: Throwable => {
        e.printStackTrace()
        wIter.queryFlow = new OperatorNode("", List(), None)
      }
    }
    results.close()

    new WebQueryResult(wIter)
  }

  private def handleExplain(explain: Explain): WebStringResult = {
    val raw = db.convert(explain.getSelectBody())._1;
    val op = db.optimize(raw)
    val res = "------ Raw Query ------\n"+
      raw.toString()+"\n"+
      "--- Optimized Query ---\n"+
      op.toString

    new WebStringResult(res)
  }

  def getAllTables: List[String] = {
    db.backend.getAllTables()
  }

  def getAllLenses: List[String] = {
    val iter = db.query(
      """
        SELECT *
        FROM MIMIR_LENSES
      """)

    val lensNames = new ListBuffer[String]()

    iter.open()
    while(iter.getNext()) {
      lensNames.append(iter(0).asString)
    }
    iter.close()

    lensNames.toList
  }

  def getAllSchemas: Map[String, List[(String, Type.T)]] = {
    getAllTables.map{ (x) => (x, db.getTableSchema(x).get) }.toMap ++
      getAllLenses.map{ (x) => (x, db.getLens(x).schema()) }.toMap
  }

  def getAllDBs: Array[String] = {
    val curDir = new File(".", "databases")
    curDir.listFiles().filter( f => f.isFile && f.getName.endsWith(".db")).map(x => x.getName)
  }

  def getExplainObject(query: String, row: String, idx: Int): String = 
  {
    val parser: MimirJSqlParser = new MimirJSqlParser(new StringReader(query));
    val oper = 
      try {
        val stmt: Statement = parser.Statement();
        if(stmt.isInstanceOf[Select]){
          db.convert(stmt.asInstanceOf[Select])
        } else {
          throw new Exception("getVGTerms got statement that is not SELECT")
        }

      } catch {
        case e: Throwable => {
          e.printStackTrace()
          return JSONBuilder.dict(List(("error", JSONBuilder.string(e.getMessage))))
        }
      }

    val explanation: Explanation =
      if(idx < 0){
        db.explainRow(oper, RowIdPrimitive(row))
      } else {
        val schema = Typechecker.schemaOf(oper)
        db.explainCell(oper, RowIdPrimitive(row), schema(idx)._1)
      }

    return explanation.toJSON

  }

  def getVGTerms(query: String, row: String, ind: Int): List[Reason] = {
    val source = new StringReader(query)
    val parser = new MimirJSqlParser(source)

    val raw =
      try {
        val stmt: Statement = parser.Statement();
        if(stmt.isInstanceOf[Select]){
          db.convert(stmt.asInstanceOf[Select])
        } else {
          throw new Exception("getVGTerms got statement that is not SELECT")
        }

      } catch {
        case e: Throwable => {
          e.printStackTrace()
          return List()
        }
      }

    val rowQuery = 
      mimir.algebra.Select(
        Comparison(Cmp.Eq, Var("ROWID_MIMIR"), new RowIdPrimitive(row)),
        raw
      )
    // println("QUERY: "+rowQuery);

    val iterator = db.queryLineage(rowQuery);

    if(!iterator.getNext()){
      throw new SQLException("Invalid Source Data ROWID: '" +row+"'");
    }
    iterator.reason(ind)
  }

  def nameForQuery(query: String): WebResult =
  {
    val source = new StringReader(query)
    val parser = new MimirJSqlParser(source)

    val rawQuery =
      try {
        val stmt: Statement = parser.Statement();
        if(stmt.isInstanceOf[Select]){
          db.convert(stmt.asInstanceOf[Select])
        } else {
          throw new Exception("nameForQuery got statement that is not SELECT")
        }

      } catch {
        case e: Throwable => {
          e.printStackTrace()
          return new WebErrorResult("ERROR: "+e.getMessage())
        }
      }
    
    val name = QueryNamer.nameQuery(db.optimize(rawQuery))

    return new WebStringResult(name);
  }

  def close(): Unit = {
    db.backend.close()
  }

  def extractVGTerms(exp: Expression): List[String] = {
    CTables.getVGTerms(exp).map( { case VGTerm((name, _), _, _) => name } )
  }

  def convertToTree(op: Operator): OperatorNode = {
    op match {
      case Project(cols, source) => {
        val projArg = cols.filter { case ProjectArg(col, exp) => CTables.isProbabilistic(exp) }
        if(projArg.isEmpty)
          convertToTree(source)
        else {
          var params = 
            projArg.flatMap( projectArg => extractVGTerms(projectArg.input) ) 
          if(params.isEmpty) {
            convertToTree(source)
          } else {
            new OperatorNode(params.head, List(convertToTree(source)), Some(params.head))
          }
        }
      }
      case Join(lhs, rhs) => new OperatorNode("Join", List(convertToTree(lhs), convertToTree(rhs)), None)
      case Union(lhs, rhs) => new OperatorNode("Union", List(convertToTree(lhs), convertToTree(rhs)), None)
      case Table(name, schema, metadata) => new OperatorNode(name, List[OperatorNode](), None)
      case o: Operator => new OperatorNode(o.getClass.toString, o.children.map(convertToTree(_)), None)
    }
  }

  def closeBackendConnection(): Unit = {
    db.backend.close()
  }
}