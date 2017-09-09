package mimir.exec.result

import mimir.algebra._

trait ResultIterator
  extends Iterator[Row]
{
  def tupleSchema: Seq[(String, Type)]
  def annotationSchema: Seq[(String, Type)]
  def schema = tupleSchema

  lazy val schemaLookup: Map[String, (Int, Type)] = 
    tupleSchema.zipWithIndex.map { case ((name, t), idx) => (name -> (idx, t)) }.toMap
  lazy val annotationsLookup: Map[String, (Int, Type)] = 
    annotationSchema.zipWithIndex.map { case ((name, t), idx) => (name -> (idx, t)) }.toMap

  def getTupleIdx(name: String): Int = schemaLookup(name)._1
  def getAnnotationIdx(name: String): Int = annotationsLookup(name)._1

  def close(): Unit

  def hasNext(): Boolean
  def next(): Row

  override def toSeq: ResultSeq = 
    new ResultSeq(super.toIndexedSeq, tupleSchema, annotationSchema)
  override def toIndexedSeq: ResultSeq = 
    new ResultSeq(super.toIndexedSeq, tupleSchema, annotationSchema)
  override def toList: List[Row] = 
    this.toIndexedSeq.toList

  def tuples = map { _.tuple }.toIndexedSeq
}