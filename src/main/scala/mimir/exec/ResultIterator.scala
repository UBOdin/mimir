package mimir.exec;

import java.sql;
import mimir.algebra._;

abstract class ResultIterator {
  def apply(v: Int): PrimitiveValue;
  def open()
  def getNext(): Boolean;
  def close();
  def numCols: Int;
  def schema: List[(String,Type.T)];
  def map[X](fn: (PrimitiveValue) => X) =
    (0 until numCols).map( (i) => fn(this(i)) )
  def toList(): List[PrimitiveValue] =
    map( (x) => x ).toList
}