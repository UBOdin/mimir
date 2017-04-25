package mimir.exec.stream

import mimir.algebra._

trait ResultIterator
  extends Iterator[Row]
{
  def schema: Seq[(String, Type)]
  def annotations: Seq[(String, Type)]

  val schemaLookup: Map[String, (Int, Type)] = 
    schema.zipWithIndex.map { case ((name, t), idx) => (name -> (idx, t)) }.toMap
  val annotationsLookup: Map[String, (Int, Type)] = 
    annotations.zipWithIndex.map { case ((name, t), idx) => (name -> (idx, t)) }.toMap

  def getIdx(name: String): Int = schemaLookup(name)._1
  def getAnnotationIdx(name: String): Int = annotationsLookup(name)._1

  def close(): Unit

  def hasNext(): Boolean
  def next(): Row
}