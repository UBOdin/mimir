package mimir.exec;

import mimir.algebra._;
import mimir.ctables.Reason;

class BagUnionResultIterator(lhs: ResultIterator, rhs: ResultIterator) extends ResultIterator
{
  var inLHS = true;

  def currIter() = if(inLHS){ lhs } else { rhs }

  def apply(v: Int): PrimitiveValue = 
    currIter().apply(v);
  def deterministicRow(): Boolean =
    currIter().deterministicRow();
  def deterministicCol(v: Int): Boolean =
    currIter().deterministicCol(v);
  def missingRows(): Boolean =
    (lhs.missingRows() || rhs.missingRows());
  def open() =
    { lhs.open(); rhs.open(); }
  def getNext(): Boolean = {
    if(inLHS) { 
      if(lhs.getNext()) { return true; }
      inLHS = false;
    }
    return rhs.getNext();
  }
  def close() =
    { lhs.close(); rhs.close(); }
  def numCols: Int =
    currIter().numCols;
  def schema: List[(String,Type)] =
    currIter().schema;

  override def reason(ind: Int): List[Reason] = {
    currIter().reason(ind)
  }
  def provenanceToken() = new RowIdPrimitive(
    currIter().provenanceToken().payload.toString
    +(if(inLHS){ ".left" } else { ".right" })
  );
}