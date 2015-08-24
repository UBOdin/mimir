package mimir.exec;

import mimir.algebra._;

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
  def schema: List[(String,Type.T)] =
    currIter().schema;

  override def reason(ind: Int): List[(String, String)] = {
    currIter().reason(ind)
  }
}