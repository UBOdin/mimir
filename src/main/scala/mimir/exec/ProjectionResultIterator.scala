package mimir.exec;

import scala.collection.mutable.ArraySeq

import java.sql._;
import mimir.algebra._;
import mimir.ctables._;
import mimir.sql.Backend;

class ProjectionResultIterator(
    src: ResultIterator, cols: List[(String, Expression)], 
    ivmanager: IViewManager, backend: Backend
  ) 
  extends ResultIterator
{
  val tuple: ArraySeq[PrimitiveValue] = 
    new ArraySeq[PrimitiveValue](src.numCols);
  
  val schema = cols.map( _ match { case (name, expr) => 
      ( name, 
        expr.exprType(src.schema.toMap[String,Type.T])
      )
    }).toList
  val exprs = cols.map( x => compile(x._2) )
  
  def apply(i: Int) = tuple(i)
  def numCols = cols.length
  
  def inputVar(i: Int) = src(i)
  
  def open(): Unit = 
  {
    src.open();
  }
  def getNext(): Boolean = 
  {
    if(!src.getNext()) { return false; }
    (0 until tuple.length).foreach( (i) => {
      tuple(i) = Eval.eval(exprs(i))
    })
    return true;
  }
  def close(): Unit = 
  {
    src.close();
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
        val analysis = ivmanager.analyze(pvar);
        compile(new AnalysisMLEProjection(
          backend, analysis, pvar.params
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

class AnalysisMLEProjection(backend: Backend, analysis: CTAnalysis, args: List[Expression])
  extends Proc(args)
{
  def exprType(bindings: Map[String,Type.T]) = analysis.varType(backend);
  def rebuild(x: List[Expression]) = new AnalysisMLEProjection(backend, analysis, x);
  def get() = analysis.computeMLE(backend, args.map(Eval.eval(_)));
}