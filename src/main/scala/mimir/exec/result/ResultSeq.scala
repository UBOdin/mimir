package mimir.exec.result

import mimir.algebra._

class ResultSeq(
  data: IndexedSeq[Row], 
  val tupleSchema: Seq[(String, Type)], 
  val annotationSchema: Seq[(String, Type)]
)
  extends IndexedSeq[Row]
{
  lazy val schemaLookup: Map[String, (Int, Type)] = 
    tupleSchema.zipWithIndex.map { case ((name, t), idx) => (name -> (idx, t)) }.toMap
  lazy val annotationsLookup: Map[String, (Int, Type)] = 
    annotationSchema.zipWithIndex.map { case ((name, t), idx) => (name -> (idx, t)) }.toMap

  def getTupleIdx(name: String): Int = schemaLookup(name)._1
  def getAnnotationIdx(name: String): Int = annotationsLookup(name)._1

  def length = data.length
  def apply(idx: Int) = data(idx)
  override def iterator = data.iterator

  def apply(colName: String): Seq[PrimitiveValue] = 
    column(colName)

  def column(colName: String): Seq[PrimitiveValue] = 
    column(getTupleIdx(colName))

  def column(idx: Int): Seq[PrimitiveValue] =
    data.map { _(idx) }

}