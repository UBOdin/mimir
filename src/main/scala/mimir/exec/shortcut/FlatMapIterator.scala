package mimir.exec.shortcut

import mimir.algebra._
import mimir.exec.result._

class FlatMapIterator(
  val tupleSchema: Seq[(ID, Type)], 
  map:(Row => Seq[Row]), 
  source: ResultIterator
) 
  extends ResultIterator
{
  var buffer = List[Row]()

  def stock()
  {
    while(buffer.isEmpty && source.hasNext){
      buffer = map(source.next()).toList
    }
  }

  def annotationSchema: Seq[(ID, Type)] = Seq()
  def close() { source.close() }
  def hasNext(): Boolean = { stock(); !buffer.isEmpty }
  def next(): Row = { 
    stock()
    buffer match {
      case Nil => null
      case head :: rest => { buffer = rest; head }
    }
  }
}