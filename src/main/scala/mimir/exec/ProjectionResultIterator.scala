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
  val deterministicExprs = cols.
    filter( _._1 != "__MIMIR_CONDITION" ).
    map( x => compileDeterministic(x._2) )
  val cond = 
    cols.find( _._1 == "__MIMIR_CONDITION" ).
         map( x => compile(x._2) )
  val deterministicCond = 
    cols.find( _._1 == "__MIMIR_CONDITION" ).
         map( x => compileDeterministic(x._2) )
  
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
  def deterministicCol(i: Int): Boolean =
    Eval.evalBool(deterministicExprs(i))
  
  def inputVar(i: Int) = src(i)
  def inputDet(i: Int) = src.deterministicCol(i)
  
  
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
      } else {
        haveMissingRows = haveMissingRows || !deterministicRow
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
  
  def compileDeterministic(expr: Expression): Expression =
  {
    if(!CTAnalysis.isProbabilistic(expr)){
      return BoolPrimitive(true)
    }
    expr match { 
      
      case CaseExpression(caseClauses, elseClause) =>
        CaseExpression(
          caseClauses.map( (clause) =>
            WhenThenClause(
              Arith.makeAnd(
                compileDeterministic(clause.when),
                compile(clause.when)
              ),
              compileDeterministic(clause.then)
            )
          ),
          compileDeterministic(elseClause)
        )
      
      
      case Arithmetic(Arith.And, l, r) =>
        Arith.makeOr(
          Arith.makeAnd(
            compileDeterministic(l),
            Not(compile(l))
          ),
          Arith.makeAnd(
            compileDeterministic(r),
            Not(compile(r))
          )
        )
      
      case Arithmetic(Arith.Or, l, r) =>
        Arith.makeOr(
          Arith.makeAnd(
            compileDeterministic(l),
            compile(l)
          ),
          Arith.makeAnd(
            compileDeterministic(r),
            compile(r)
          )
        )
      
      case pvar: PVar =>
        BoolPrimitive(false)
      
      case Var(v) => {
        val idx = 
          src.schema.indexWhere( 
            _._1.toUpperCase == v
          )
        if(idx < 0){
          throw new SQLException("Invalid schema: "+v+" not in "+src.schema)
        }
        new VarIsDeterministic(this, idx)
      }

      case _ => expr.children.
                  map( compileDeterministic(_) ).
                  fold(
                    BoolPrimitive(true)
                  )( 
                    Arith.makeAnd(_,_) 
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

class VarIsDeterministic(src: ProjectionResultIterator, idx: Int)
  extends Proc(List[Expression]())
{
  def exprType(bindings: Map[String,Type.T]) = Type.TBool;
  def rebuild(x: List[Expression]) = new VarIsDeterministic(src, idx)
  def get() = BoolPrimitive(src.inputDet(idx))
}

class AnalysisMLEProjection(analysis: CTAnalysis, args: List[Expression])
  extends Proc(args)
{
  def exprType(bindings: Map[String,Type.T]) = analysis.varType;
  def rebuild(x: List[Expression]) = new AnalysisMLEProjection(analysis, x);
  def get() = analysis.computeMLE(args.map(Eval.eval(_)));
}