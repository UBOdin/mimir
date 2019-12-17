package mimir.exec.shortcut

import scala.collection.mutable.PriorityQueue
import mimir.algebra._
import mimir.exec.result._

object ComputeTopK
{
  def apply(k: Long, order: SortOrdering, source: ResultIterator): Seq[Seq[PrimitiveValue]] =
  {
    // Set up a min-heap
    val topK = new PriorityQueue()(new Ordering[Row] { def compare(a: Row, b: Row) = order.compare(b, a) })

    for(row <- source){
      topK += row
      if(topK.size > k) { topK.dequeue }
    }

    return topK.dequeueAll.map { _.tuple }
  }
}