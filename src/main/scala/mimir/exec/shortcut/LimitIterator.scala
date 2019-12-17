package mimir.exec.shortcut

import mimir.exec.result._

class LimitIterator(
  limit: Long,
  source: ResultIterator
) extends ResultIterator
{
  var idx = 0;

  def close() {}
  def hasNext(): Boolean = idx < limit
  def next(): Row = {
    if(!hasNext){ return null }
    idx += 1
    return source.next
  }
  def annotationSchema = source.annotationSchema
  def tupleSchema = source.tupleSchema
}