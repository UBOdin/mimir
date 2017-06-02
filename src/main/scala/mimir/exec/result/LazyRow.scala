package mimir.exec.result

import mimir.algebra._


case class LazyRow(
  input: Seq[PrimitiveValue], 
  tupleDefinition: Seq[Seq[PrimitiveValue]=>PrimitiveValue],
  annotationDefinition: Seq[Seq[PrimitiveValue]=>PrimitiveValue],
  val tupleSchema: Seq[(String, Type)],
  val annotationIndexes: Map[String,Int]
) extends Row {
  def tuple: Seq[PrimitiveValue] = 
    tupleDefinition.map { _(input) }
  def apply(idx: Int): PrimitiveValue = 
    tupleDefinition(idx)(input)
  def annotation(name: String): PrimitiveValue = 
    annotation(annotationIndexes(name))
  def annotation(idx: Int): PrimitiveValue = 
    annotationDefinition(idx)(input)
}
