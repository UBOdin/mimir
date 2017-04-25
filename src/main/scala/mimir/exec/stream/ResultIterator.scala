package mimir.exec.stream

import mimir.algebra._

trait ResultIterator
  extends Iterator[Row]
{
  def schema: Seq[(String, (Int, Type))]
  def annotations: Seq[(String, (Int, Type))]

  val schemaLookup: Map[String, (Int, Type)]      = schema.toMap
  val annotationsLookup: Map[String, (Int, Type)] = annotations.toMap

  def getIdx(name: String): Int = schemaLookup(name)._1
  def getAnnotationIdx(name: String): Int = annotationsLookup(name)._1

  def close(): Unit

  def hasNext(): Boolean
  def next(): Row
}