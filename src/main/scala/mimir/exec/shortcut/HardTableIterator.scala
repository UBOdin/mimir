package mimir.exec.shortcut

import mimir.algebra._
import mimir.exec.result._

class HardTableIterator(
  val tupleSchema: Seq[(ID, Type)],
  data: Seq[Seq[PrimitiveValue]]
) extends ResultIterator
{
  var idx = -1

  def annotationSchema: Seq[(ID, Type)] = Seq()
  def close() {}
  def hasNext(): Boolean = { idx < data.size-1 }
  def next(): Row = { 
    if(hasNext()){
      idx += 1
      ExplicitRow(data(idx), Seq(), this) 
    } else { null }
  }
}