package mimir.exec.stream

import mimir.algebra._
import mimir.provenance._
import mimir.ctables._


trait Row
{
  def tuple: Seq[PrimitiveValue]
  def tupleMap: Map[String,PrimitiveValue] =
    tupleSchema.zip(tuple).map { x => (x._1._1 -> x._2) }.toMap

  def apply(idx: Int): PrimitiveValue
  def apply(name: String): PrimitiveValue = 
    apply(tupleSchema.indexWhere( _._1.equals(name) ))

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

  override def toString: String = "<" + tuple.mkString(", ") + ">"
}

case class ExplicitRow(val tuple: Seq[PrimitiveValue], val annotations: Seq[PrimitiveValue], val source: ResultIterator)
  extends Row
{
  def apply(idx: Int): PrimitiveValue = tuple(idx)

  def annotation(idx: Int): PrimitiveValue = annotation(idx)
  def annotation(name: String): PrimitiveValue = annotation(source.getAnnotationIdx(name))

  def tupleSchema = source.schema;
}