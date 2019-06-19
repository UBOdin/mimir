package mimir.exec.result

import mimir.algebra._

class UnionResultIterator(src: Iterator[ResultIterator]) extends ResultIterator
{
  var curr = src.next;

  def tupleSchema: Seq[(ID, Type)] = curr.tupleSchema
  def annotationSchema: Seq[(ID, Type)] = curr.annotationSchema

  def close(): Unit =
  {
    curr.close()
    src.foreach { _.close }
  }

  def hasNext(): Boolean =
  {
    while(!curr.hasNext() && src.hasNext){ curr.close(); curr = src.next }
    return curr.hasNext()
  }
  def next(): Row = 
    curr.next()
}