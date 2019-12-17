package mimir.exec.shortcut

import scala.collection.mutable.PriorityQueue

import mimir.algebra._
import mimir.exec.result._

class HeapSortIterator(
  val tupleSchema: Seq[(ID, Type)],
  heap: PriorityQueue[Row]
) extends ResultIterator
{
  def hasNext() = heap.isEmpty
  def next() = heap.dequeue
  def annotationSchema: Seq[(ID, Type)] = Seq()
  def close() {}
}

object HeapSortIterator
{
  def apply(order: Ordering[Row], source: ResultIterator): HeapSortIterator =
  {
    val heap = new PriorityQueue()(order)
    heap ++= source
    new HeapSortIterator(source.tupleSchema, heap)
  }
}