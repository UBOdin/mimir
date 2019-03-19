package mimir.exec.result

import sparsity.Name
import mimir.algebra._

trait ResultIterator
  extends Iterator[Row]
{
  def tupleSchema: Seq[(Name, Type)]
  def annotationSchema: Seq[(Name, Type)]
  def schema = tupleSchema

  lazy val schemaLookup: Map[Name, (Int, Type)] = 
    tupleSchema.zipWithIndex.map { case ((name, t), idx) => (name -> (idx, t)) }.toMap
  lazy val annotationsLookup: Map[Name, (Int, Type)] = 
    annotationSchema.zipWithIndex.map { case ((name, t), idx) => (name -> (idx, t)) }.toMap

  def hasAnnotation(annotation: Name): Boolean = annotationsLookup contains annotation;

  def getTupleIdx(name: Name): Int = schemaLookup(name)._1
  def getAnnotationIdx(name: Name): Int = annotationsLookup(name)._1

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