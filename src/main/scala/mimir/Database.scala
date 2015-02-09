package mimir;

import java.sql._;

import mimir.sql.{Backend,SqlToRA,RAToSql,CreateIView};
import mimir.exec.{Compiler,ResultIterator,ResultSetIterator};
import mimir.ctables.{IViewManager,CTAnalysis,PVar};
import mimir.algebra._;

case class Database(backend: Backend)
{
  val sql = new SqlToRA(this)
  val ra = new RAToSql(this)
  val iviews = new IViewManager(this)
  val compiler = new Compiler(this)  
  
  
  def query(sql: String): ResultIterator = 
    new ResultSetIterator(backend.execute(sql))
  def query(sql: String, args: List[String]): ResultIterator = 
    new ResultSetIterator(backend.execute(sql, args))
  def query(sql: net.sf.jsqlparser.statement.select.Select): ResultIterator = 
    new ResultSetIterator(backend.execute(sql))
  def query(sql: net.sf.jsqlparser.statement.select.SelectBody): ResultIterator =
    new ResultSetIterator(backend.execute(sql))
  
  def update(sql: String): Unit =
    backend.update(sql);
  def update(sql: String, args: List[String]): Unit =
    backend.update(sql, args);

  def optimize(oper: Operator): Operator =
  {
    compiler.optimize(oper)
  }
  def query(oper: Operator): ResultIterator = 
  {
    compiler.compile(optimize(oper))
  }
  def queryRaw(oper: Operator): ResultIterator =
  {
    compiler.compile(oper);
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
    
  
  
  def analyze(pvar: PVar): CTAnalysis =
  {
    iviews.analyze(pvar)
  }
  
  
  
  def convert(sel: net.sf.jsqlparser.statement.select.Select): Operator =
    sql.convert(sel)
  def convert(sel: net.sf.jsqlparser.statement.select.SelectBody): (Operator,Map[String, String]) =
    sql.convert(sel, null)
  def convert(expr: net.sf.jsqlparser.expression.Expression): Expression =
    sql.convert(expr)
  def convert(oper: Operator): net.sf.jsqlparser.statement.select.SelectBody =
    ra.convert(oper)
  
  def getTableSchema(name: String): List[(String,Type.T)] =
    backend.getTableSchema(name);  
  def getTableOperator(table: String): Operator =
    backend.getTableOperator(table)
  def getTableOperator(table: String, metadata: Map[String, Type.T]): Operator =
    backend.getTableOperator(table, metadata)
  
  def createIView(viewDefn: CreateIView): Unit =
    iviews.create(viewDefn)
  
  def initializeDBForMimir(): Unit =
    iviews.init();
  
  def loadState(): Unit =
    iviews.load();
  
  def getView(name: String): Option[(Operator)] =
  {
    // System.out.println("Selecting from ..."+name);
    if(iviews == null){ None }
    else {
      //println(iviews.views.toString())
      iviews.views.get(name.toUpperCase()) match {
        case None => None
        case Some(view) => 
          // println("Found: "+name); 
          Some(view.get())
      }
    }
  }
}