package mimir.exec.result

import mimir.algebra._

class ResultSeq(
  data: scala.collection.immutable.IndexedSeq[Row], 
  val tupleSchema: Seq[(ID, Type)], 
  val annotationSchema: Seq[(ID, Type)]
)
  extends scala.collection.immutable.IndexedSeq[Row]
{
  lazy val schemaLookup: Map[ID, (Int, Type)] = 
    tupleSchema.zipWithIndex.map { case ((name, t), idx) => (name -> (idx, t)) }.toMap
  lazy val annotationsLookup: Map[ID, (Int, Type)] = 
    annotationSchema.zipWithIndex.map { case ((name, t), idx) => (name -> (idx, t)) }.toMap

  def getTupleIdx(name: ID): Int = schemaLookup(name)._1
  def getAnnotationIdx(name: ID): Int = annotationsLookup(name)._1

  def length = data.length
  def apply(idx: Int) = data(idx)
  override def iterator = data.iterator

  def apply(colName: ID): Seq[PrimitiveValue] = 
    column(colName)

  def column(colName: ID): Seq[PrimitiveValue] = 
    column(getTupleIdx(colName))

  def column(idx: Int): Seq[PrimitiveValue] =
    data.map { _(idx) }

  def tuples = map { _.tuple }
}