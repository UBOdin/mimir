package mimir.exec.result

import mimir.algebra._

case class ExplicitRow(val tuple: Seq[PrimitiveValue], val annotations: Seq[PrimitiveValue], val source: ResultIterator)
  extends Row
{
  def apply(idx: Int): PrimitiveValue = tuple(idx)

  def annotation(idx: Int): PrimitiveValue = if(idx < annotations.size) { annotations(idx) } else { NullPrimitive() }
  def annotation(name: String): PrimitiveValue = if(source.hasAnnotation(name)){ annotation(source.getAnnotationIdx(name)) } else { NullPrimitive() }

  def tupleSchema = source.schema;
}