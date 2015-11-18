package mimir

import java.io.{File, StringReader}
import java.sql.SQLException

import mimir.algebra._
import mimir.ctables.{CTPercolator, CTables, VGTerm}
import mimir.exec.ResultSetIterator
import mimir.parser.MimirJSqlParser
import mimir.sql.{CreateLens, Explain, JDBCBackend}
import mimir.web._
import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.insert.Insert
import net.sf.jsqlparser.statement.select.Select
import net.sf.jsqlparser.statement.update.Update
import net.sf.jsqlparser.util.deparser.{InsertDeParser, ExpressionDeParser, SelectDeParser, UpdateDeParser}

import scala.collection.mutable.ListBuffer

class WebAPI(dbName: String = "debug.db", backend: String = "sqlite") {


  val db = new Database(dbName, new JDBCBackend(backend, dbName))

  def openBackendConnection(): Unit = {
    db.backend.open()
  }

  def getCurrentDB = dbName

  def initializeDBForMimir() = {
    db.initializeDBForMimir()
  }

  def handleLoadTable(filename: String) = {
    db.handleLoadTable(filename.replace(".csv", ""), filename)
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
    val results = db.query(CTPercolator.propagateRowIDs(raw, true))
    val resultsT = System.nanoTime()

    println("Convert time: "+((start-rawT)/(1000*1000))+"ms")
    println("Compile time: "+((rawT-resultsT)/(1000*1000))+"ms")

    results.open()
    val wIter: WebIterator = db.generateWebIterator(results)
    try{
      wIter.queryFlow = convertToTree(raw)
    } catch {
      case e: Throwable => {
        e.printStackTrace()
        wIter.queryFlow = new OperatorNode("", List(), List())
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

  def getAllSchemas: Map[String, List[(String, Type.T)]] = {
    getAllTables.map{ (x) => (x, db.getTableSchema(x).get) }.toMap ++
      getAllLenses.map{ (x) => (x, db.getLens(x).schema()) }.toMap
  }

  def getAllDBs: Array[String] = {
    val curDir = new File(".", "databases")
    curDir.listFiles().filter( f => f.isFile && f.getName.endsWith(".db")).map(x => x.getName)
  }

  def getVGTerms(query: String, row: String, ind: Int): List[(String, String)] = {
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

    val iterator = db.query(
      CTPercolator.percolate(
        mimir.algebra.Select(
          Comparison(Cmp.Eq, Var("ROWID"), new RowIdPrimitive(row.substring(1, row.length - 1))),
          db.optimize(raw)
        )
      )
    )

    if(!iterator.getNext()){
      throw new SQLException("Invalid Source Data ROWID: '" +row+"'");
    }
    iterator.reason(ind)

    /* Really, this should be data-aware.  The query should
       get the VG terms specifically affecting the specific
       row in question.  See #32 */
//    val sch = optimized.schema
//    val col_defn = if(ind == -1) sch(sch.length - 1) else sch(ind)
//
//    val ret =
//    OperatorUtils.columnExprForOperator(col_defn._1, optimized).
//      // columnExprForOperator returns (expr,oper) pairs.  We care
//      // about the expression
//      map( _._1 ).
//      // Get the VG terms that might be affecting it.  THIS SHOULD
//      // BE DATA-DEPENDENT.
//      map( db.getVGTerms(_) ).
//      // List of lists -> Just a list
//      flatten.
//      // Pull the reasons
//      map( _.reason() ).
//      // Discard duplicate reasons
//      distinct
//    ret
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
    exp match {
      case Not(child) => extractVGTerms(child)
      case VGTerm((name, model), idx, args) => List(name)
      case CaseExpression(wtClauses, eClause) => {
        var wt = List[String]()
        for (i <- wtClauses.indices) {
          val wclause = extractVGTerms(wtClauses(i).when)
          val tclause = extractVGTerms(wtClauses(i).then)
          wt = wt ++ wclause ++ tclause
        }
        wt ++ extractVGTerms(eClause)
      }
      case Arithmetic(o, l, r) => extractVGTerms(l) ++ extractVGTerms(r)
      case Comparison(o, l, r) => extractVGTerms(l) ++ extractVGTerms(r)
      case IsNullExpression(child, neg) => extractVGTerms(child)
      case Function(op, params) => List(op)
      case _ => List()
    }
  }

  def convertToTree(op: Operator): OperatorNode = {
    op match {
      case Project(cols, source) => {
        val projArg = cols.filter { case ProjectArg(col, exp) => CTables.isProbabilistic(exp) }
        if(projArg.isEmpty)
          convertToTree(source)
        else {
          var params = List[String]()
          projArg.foreach{ case ProjectArg(col, exp) => params = params ++ extractVGTerms(exp) }
          val p = params.toSet.filter(a => db.lenses.lensCache.contains(a)).map(b => (b, db.lenses.lensCache.get(b).get.lensType))
          if(p.isEmpty)
            convertToTree(source)
          else {
            val (name, lensType) = p.head
            new OperatorNode(name, List(convertToTree(source)), List(lensType))
          }
        }
      }
      case Join(lhs, rhs) => new OperatorNode("Join", List(convertToTree(lhs), convertToTree(rhs)), List())
      case Union(isAll, lhs, rhs) => new OperatorNode("Union" + (if(isAll) "(ALL)" else "(DISTINCT)"), List(convertToTree(lhs), convertToTree(rhs)), List())
      case Table(name, schema, metadata) => new OperatorNode(name, List[OperatorNode](), List())
      case o: Operator => convertToTree(o.children(0))
    }
  }

  def closeBackendConnection(): Unit = {
    db.backend.close()
  }
}