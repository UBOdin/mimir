package mimir.exec;

import scala.collection.mutable.ArraySeq

import java.sql._;
import mimir.algebra._;
import mimir.ctables._;
import mimir.optimizer.InlineVGTerms;
import mimir.Database;

class ProjectionResultIterator(
    db: Database,
    val src: ResultIterator,
    cols: List[(String, Expression)]
  ) 
  extends ResultIterator
{
  /**
   * Instrumentation
   */
  var counter = 0

  /**
   * The output schema of this iterator
   */
  val schema = {
    val srcSchema = src.schema.toMap[String,Type.T]
    cols.map( _ match { case (name, expr) => 
      ( name, 
        expr.exprType(srcSchema)
      )
    }).filter( _._1 != CTables.conditionColumn )
  }
  /**
   * Maximal Likelihood Expected value expressions for 
   * the projection's output.  Excludes the condition
   * column (but see cond, below).
   */
  val exprs = cols.
    filter( _._1 != CTables.conditionColumn ).
    map( x => VarProjection.compile(src, InlineVGTerms.optimize(x._2)) )
  val exprsLineage = cols.
    filter( _._1 != CTables.conditionColumn ).
    map( x => VarProjection.compile(src, x._2) )

  /**
   * Boolean expressions that determine whether a given
   * column is deterministic for a given row.  If 
   * deterministicExprs(i) evaluates to true for the 
   * current row, then schema(i) is deterministic.
   * If this expression evaluates to false, then the
   * corresponding column has an "asterisk".
   */
  val deterministicExprs = cols.
    filter( _._1 != CTables.conditionColumn ).
    map( x => VarProjection.compile(src, CTAnalyzer.compileDeterministic(x._2)) )

  /**
   * Boolean expression that determines the presence
   * of the current row in the query output under a 
   * maximal likelihood assumption.  If this expression
   * evaluates to false, the row is <b>most likely</b>
   * not part of the result set and should be dropped.
   */
  val cond = 
    cols.find( _._1 == CTables.conditionColumn ).
         map( x => VarProjection.compile(src, InlineVGTerms.optimize(x._2)) )
  val condLineage = 
    cols.find( _._1 == CTables.conditionColumn ).
         map( x => VarProjection.compile(src, x._2) )

  /**
   * Boolean expression that determines whether `cond`
   * is deterministic for the current row.  If this
   * expression evaluates to false, there is some chance
   * that the maximal likelihood assumption is wrong
   * about the presence of this tuple in the result set
   * (i.e., the entire tuple has an asterisk)
   */
  val deterministicCond = 
    cols.find( _._1 == CTables.conditionColumn ).
         map( x => VarProjection.compile(src, CTAnalyzer.compileDeterministic(x._2)) )
  
  val tuple: ArraySeq[PrimitiveValue] = 
    new ArraySeq[PrimitiveValue](exprs.length);
  
  var haveMissingRows = false;
  
  def apply(i: Int) = tuple(i)
  def numCols = tuple.length
  def missingRows() = haveMissingRows || src.missingRows
  
  def deterministicRow(): Boolean = 
    deterministicCond match {
      case Some(c) => Eval.evalBool(c)
      case None => true
    }
  def deterministicCol(i: Int): Boolean = {
    val bindings = Map[String, PrimitiveValue](CTables.SEED_EXP -> IntPrimitive(1))
    Eval.evalBool(deterministicExprs(i), bindings)
  }
  
  def inputVar(i: Int) = src(i)
  def inputDet(i: Int) = src.deterministicCol(i)
  
  
  def open(): Unit = 
    src.open()
  def close(): Unit = 
    src.close()
  def getNext(): Boolean = 
  {
    var searching = true
    while(searching){
      if(!src.getNext()) { println("Total scanned: "+counter); return false; }
      if(cond match {
        case None => true
        case Some(c) => Eval.evalBool(c)
      }) {
        tuple.indices.foreach( (i) => {
          tuple(i) = Eval.eval(exprs(i))
        })
        searching = false
      } else {
        haveMissingRows = haveMissingRows || !deterministicRow
      }
      counter = counter + 1
    }
    true
  }

  override def reason(ind: Int): List[(String, String)] = {
    val expr: Expression = if(ind == -1) condLineage.get else exprsLineage(ind)
    val evaluated = Eval.inline(expr)
    db.getVGTerms(evaluated).map((vgterm) => vgterm.reason()).distinct
  }
}
