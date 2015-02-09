package mimir.exec;

import scala.collection.mutable.ArraySeq

import java.sql._;
import mimir.algebra._;
import mimir.ctables._;
import mimir.Database;

class ProjectionResultIterator(
    db: Database,
    src: ResultIterator, 
    cols: List[(String, Expression)]
  ) 
  extends ResultIterator
{

  val schema = {
    val srcSchema = src.schema.toMap[String,Type.T];
    cols.map( _ match { case (name, expr) => 
      ( name, 
        expr.exprType(srcSchema)
      )
    }).filter( _._1 != "__MIMIR_CONDITION" ).
    toList
  }
  val exprs = cols.
    filter( _._1 != "__MIMIR_CONDITION" ).
    map( x => compile(x._2) )
  val cond = 
    cols.find( _._1 == "__MIMIR_CONDITION" ).
         map( x => compile(x._2) )
  var condSatisfied: Boolean = true;
  
  val tuple: ArraySeq[PrimitiveValue] = 
    new ArraySeq[PrimitiveValue](exprs.length);
  
  def apply(i: Int) = tuple(i)
  def numCols = tuple.length
  
  def inputVar(i: Int) = src(i)
  
  def open(): Unit = 
    src.open();
  def close(): Unit = 
    src.close();
  def getNext(): Boolean = 
  {
    var searching = true;
    while(searching){
      if(!src.getNext()) { return false; }
      if(cond match {
        case None => true
        case Some(c) => Eval.evalBool(c)
      }) {
        (0 until tuple.length).foreach( (i) => {
          tuple(i) = Eval.eval(exprs(i))
        })
        searching = false;
      }
    }
    return true;
  }

  def compile(expr: Expression): Expression =
  {
    expr match { 
      case Var(v) => 
        val idx = 
          src.schema.indexWhere( 
            _._1.toUpperCase == v
          )
        if(idx < 0){
          throw new SQLException("Invalid schema: "+v+" not in "+src.schema)
        }
        val t = src.schema(idx)._2
        new VarProjection(this, idx, t)

      case pvar: PVar =>
        val analysis = db.analyze(pvar);
        compile(new AnalysisMLEProjection(
            analysis, 
            pvar.params
          ));
      
      case _ => 
        expr.rebuild(
          expr.children.map(compile(_))
        )
    }
  }
}

class VarProjection(src: ProjectionResultIterator, idx: Int, t: Type.T)
  extends Proc(List[Expression]())
{
  def exprType(bindings: Map[String,Type.T]) = t;
  def rebuild(x: List[Expression]) = new VarProjection(src, idx, t)
  def get() = src.inputVar(idx);
}

class AnalysisMLEProjection(analysis: CTAnalysis, args: List[Expression])
  extends Proc(args)
{
  def exprType(bindings: Map[String,Type.T]) = analysis.varType;
  def rebuild(x: List[Expression]) = new AnalysisMLEProjection(analysis, x);
  def get() = analysis.computeMLE(args.map(Eval.eval(_)));
}