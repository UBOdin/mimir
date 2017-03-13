package mimir.exec;

import scala.collection.mutable.ArraySeq

import java.sql._;
import mimir.algebra._;
import mimir.ctables._;
import mimir.Database;

class VarProjection(src: ResultIterator, idx: Int, t: Type)
  extends Proc(Seq[Expression]())
{
  def getType(bindings: Seq[Type]) = t;
  def rebuild(x: Seq[Expression]) = new VarProjection(src, idx, t)
  def get(v:Seq[PrimitiveValue]) = src(idx)

  override def toString = src.schema(idx)._1+":"+idx
}

class ProvenanceProjection(src: ResultIterator)
  extends Proc(Seq[Expression]())
{
  def getType(bindings: Seq[Type]) = TRowId();
  def rebuild(x: Seq[Expression]) = new ProvenanceProjection(src)
  def get(v:Seq[PrimitiveValue]): PrimitiveValue = src.provenanceToken
}

object VarProjection
{
  /**
   * Compile an expression for evaluation.  mimir.algebra.Eval
   * can already handle most Expression objects, except for 
   * Var and PVar.  We replace Var instances with VarProjection
   * instances that are direct references to the columns being
   * produced by `src` (and reference by index, rather than)
   * by name.  We replace PVar instances with Expectations 
   * constructed from the PVar's definition (obtained from 
   * db.analyze()).
   */
  def compile(src: ResultIterator, expr: Expression): Expression =
  {
    expr match {
      case Var(v) =>
        v match {
          case _ => {
            val idx =
              src.schema.indexWhere(
                _._1.toUpperCase == v
              )
            if(idx < 0){
              throw new SQLException("Invalid schema: "+v+" not in "+src.schema)
            }
            val t = src.schema(idx)._2
            new VarProjection(src, idx, t)
          }
        }
      
      case _ =>
        expr.rebuild(
          expr.children.map(compile(src, _))
        )
    }
  }
}