package mimir.exec;

import mimir.algebra._;
import mimir.models.Reason;

class BagUnionResultIterator(elems: List[ResultIterator]) extends ResultIterator
{
  var curr = elems

  def apply(v: Int): PrimitiveValue = 
    curr.head.apply(v);
  def deterministicRow(): Boolean =
    curr.head.deterministicRow();
  def deterministicCol(v: Int): Boolean =
    curr.head.deterministicCol(v);
  def missingRows(): Boolean =
    elems.exists(_.missingRows())
  def open() =
    { elems.foreach(_.open()) }
  def getNext(): Boolean = {
    if(curr == Nil){ return false }
    while(!curr.head.getNext()){ 
      curr = curr.tail
      if(curr == Nil){ return false }
    }
    return true;
  }
  def close() =
    { elems.foreach(_.close()) }
  def numCols: Int =
    curr.head.numCols;
  def schema: List[(String,Type.T)] =
    curr.head.schema;

  override def reason(ind: Int): List[Reason] = {
    curr.head.reason(ind)
  }
  def provenanceToken() = new RowIdPrimitive(
    curr.head.provenanceToken().payload.toString
  );
}