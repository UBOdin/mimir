package mimir.exec.result

import java.sql.SQLException
import mimir.algebra._

case class LazyRow(
  input: Row, 
  tupleDefinition: Seq[Row=>PrimitiveValue],
  annotationDefinition: Seq[Row=>PrimitiveValue],
  val tupleSchema: Seq[(ID, Type)],
  val annotationIndexes: Map[ID,Int]
) extends Row {
  def tuple: Seq[PrimitiveValue] = 
    tupleDefinition.map { _(input) }
  def apply(idx: Int): PrimitiveValue = 
  {
    try { tupleDefinition(idx)(input) }
    catch { 
      case _:IndexOutOfBoundsException => 
        throw new SQLException(s"$tuple does not have a column $idx")
      case e:Throwable => 
        throw new RuntimeException(
          s"Error Decoding ${tupleSchema(idx)._1} (${tupleSchema(idx)._2})",
          e
        )
    }
  }
  def annotation(name: ID): PrimitiveValue = 
    annotation(annotationIndexes(name))
  def annotation(idx: Int): PrimitiveValue = 
    annotationDefinition(idx)(input)
}
