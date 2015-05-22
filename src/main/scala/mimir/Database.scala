package mimir;

import java.sql._;

import mimir.sql.{Backend,SqlToRA,RAToSql,CreateLens};
import mimir.exec.{Compiler,ResultIterator,ResultSetIterator};
import mimir.ctables.{VGTerm,CTPercolator,Model};
import mimir.lenses.{Lens,LensManager}
import mimir.parser.{OperatorParser}
import mimir.algebra._;

case class Database(backend: Backend)
{
  val sql = new SqlToRA(this)
  val ra = new RAToSql(this)
  val lenses = new LensManager(this)
  val compiler = new Compiler(this)  
  val operator = new OperatorParser(this.getLensModel, this.getTableSchema(_).toMap)
  
  def query(sql: String): ResultIterator = 
    new ResultSetIterator(backend.execute(sql))
  def query(sql: String, args: List[String]): ResultIterator = 
    new ResultSetIterator(backend.execute(sql, args))
  def query(sql: net.sf.jsqlparser.statement.select.Select): ResultIterator = 
    new ResultSetIterator(backend.execute(sql))
  def query(sql: net.sf.jsqlparser.statement.select.SelectBody): ResultIterator =
    new ResultSetIterator(backend.execute(sql))
  
  def update(sql: String): Unit = {
    // println(sql);
    backend.update(sql);
  }
  def update(sql: String, args: List[String]): Unit = {
    // println(sql);
    backend.update(sql, args);
  }

  def optimize(oper: Operator): Operator =
  {
    CTPercolator.percolate(
      compiler.optimize(oper)
    )
  }
  def query(oper: Operator): ResultIterator = 
  {
    compiler.compile(oper)
  }
  def queryRaw(oper: Operator): ResultIterator =
  {
    compiler.buildIterator(oper);
  }

  def dump(result: ResultIterator): Unit =
  {
    println(result.schema.map( _._1 ).mkString(","))
    println("------")
    while(result.getNext()){
      println(
        (0 until result.numCols).map( (i) => {
          result(i)+(
            if(!result.deterministicCol(i)){ "*" } else { "" }
          )
        }).mkString(",")+(
          if(!result.deterministicRow){
            " (This row may be invalid)"
          } else { "" }
        )
      )
    }
    if(result.missingRows){
      println("( There may be missing result rows )")
    }
  }
  
  
  def convert(sel: net.sf.jsqlparser.statement.select.Select): Operator =
    sql.convert(sel)
  def convert(sel: net.sf.jsqlparser.statement.select.SelectBody): (Operator,Map[String, String]) =
    sql.convert(sel, null)
  def convert(expr: net.sf.jsqlparser.expression.Expression): Expression =
    sql.convert(expr)
  def convert(oper: Operator): net.sf.jsqlparser.statement.select.SelectBody =
    ra.convert(oper)

  def parseExpression(exprString: String): Expression =
    operator.expr(exprString)
  def parseExpressionList(exprListString: String): List[Expression] =
    operator.exprList(exprListString)
  def parseOperator(operString: String): Operator =
    operator.operator(operString)

  def getTableSchema(name: String): List[(String,Type.T)] =
    backend.getTableSchema(name);  
  def getTableOperator(table: String): Operator =
    backend.getTableOperator(table)
  def getTableOperator(table: String, metadata: Map[String, Type.T]): Operator =
    backend.getTableOperator(table, metadata)

  def getLens(lensName: String): Lens =
    lenses.load(lensName).get
  def getLensModel(lensName: String): Model = 
    lenses.modelForLens(lensName)
  
  def createLens(lensDefn: CreateLens): Unit =
    lenses.create(lensDefn)
  
  def initializeDBForMimir(): Unit =
    lenses.init();
  
  def loadState(): Unit = {}
  
  def getView(name: String): Option[(Operator)] =
  {
    // System.out.println("Selecting from ..."+name);
    if(lenses == null){ None }
    else {
      //println(iviews.views.toString())
      lenses.load(name.toUpperCase()) match {
        case None => None
        case Some(lens) => 
          // println("Found: "+name); 
          Some(lens.view)
      }
    }
  }
}