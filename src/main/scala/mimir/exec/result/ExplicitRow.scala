package mimir.exec.result

import mimir.algebra._

case class ExplicitRow(val tuple: Seq[PrimitiveValue], val annotations: Seq[PrimitiveValue], val source: ResultIterator)
  extends Row
{
  def apply(idx: Int): PrimitiveValue = tuple(idx)

  def annotation(idx: Int): PrimitiveValue = annotations(idx)
  def annotation(name: String): PrimitiveValue = annotation(source.getAnnotationIdx(name))

  def tupleSchema = source.schema;
}