package mimir.exec.stream

import mimir.algebra._

case class LazyRow(
  input: Map[String,PrimitiveValue], 
  tupleDefinition: Seq[ProjectArg],
  annotationDefinition: Seq[ProjectArg],
  val tupleSchema: Seq[(String, Type)]
) extends Row {
  def tuple: Seq[PrimitiveValue] = 
    tupleDefinition.map { t => Eval.eval(t.expression, input) }
  def apply(idx: Int): PrimitiveValue = 
    Eval.eval(tupleDefinition(idx).expression, input)
  def annotation(name: String): PrimitiveValue = 
    Eval.eval(
      annotationDefinition.
        find { a => a.name.equals(name.toUpperCase) }.
        get.
        expression,
      input
    )
  def annotation(idx: Int): PrimitiveValue = 
    Eval.eval(annotationDefinition(idx).expression, input)
  
}