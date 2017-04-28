package mimir.exec.stream

import java.sql.SQLException
import mimir.algebra._
import mimir.provenance._
import mimir.ctables._


trait Row
{
  def tuple: Seq[PrimitiveValue]
  def tupleMap: Map[String,PrimitiveValue] =
    tupleSchema.zip(tuple).map { x => (x._1._1 -> x._2) }.toMap

  def apply(idx: Int): PrimitiveValue
  def apply(name: String): PrimitiveValue = {
    val idx = tupleSchema.indexWhere( _._1.equals(name) )
    if(idx < 0){
      throw new SQLException(s"Field '$name' not in tuple: ${tupleSchema.map(_._1)}")
    } else {
      return apply(idx)
    }
  }

  def annotation(idx: Int): PrimitiveValue
  def annotation(name: String): PrimitiveValue

  def tupleSchema: Seq[(String,Type)]

  def provenance: RowIdPrimitive = 
    annotation(Provenance.rowidColnameBase).asInstanceOf[RowIdPrimitive]

  def isDeterministic(): Boolean = 
    annotation(CTPercolator.mimirRowDeterministicColumnName).asInstanceOf[BoolPrimitive].v
  def isColDeterministic(col: String): Boolean = 
    annotation(CTPercolator.mimirColDeterministicColumnPrefix + col).asInstanceOf[BoolPrimitive].v
  def isColDeterministic(idx: Int): Boolean =
    isColDeterministic(tupleSchema(idx)._1)

  override def toString: String = "<" + 
    tupleSchema.zip(tuple).map { case ((name,_),v) => name+":"+v }.mkString(", ") + 
  ">"
}