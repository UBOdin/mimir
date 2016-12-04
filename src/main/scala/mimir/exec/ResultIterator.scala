package mimir.exec;

import mimir.algebra._;
import mimir.models.Reason;

/**
 * An abstract interface for result iterators.  The level of 
 * complexity is somewhere between java's native iterators and
 * the overwhelming nonsense that is JDBC.  
 *
 * For general use, the following pattern should be used:
 * 
 * val iterator = ...
 * iterator.open()
 * while(iterator.getNext()){
 *   // iterator(n) returns the nth (0 = first) column of the current row.
 *   // iterator.deterministicRow() returns true if the current row is deterministic
 *   // etc...
 * }
 * iterator.close()
 * 
 * TODO: 
 *  Make this a TraversableOnce
 */

abstract class ResultIterator {
  /**
   * Prepare the result iterator for use and position the cursor 
   * **before** the first row to be returned.  In other words,
   * the cursor will not point at valid data until getNext() is
   * called at least once.  
   */
  def open()
  /**
   * Advance the cursor; Return false if there are no more rows.
   */
  def getNext(): Boolean;
  /**
   * Release resources allocated with the result iterator.
   * Must be called once the iterator is no longer needed.
   */
  def close();

  /**
   * Scala shorthand for operator().  Get the value for column N in the current row
   * e.g., iterator(0) (or equivalently iterator.apply(0)) returns the first column.
   */
  def apply(v: Int): PrimitiveValue;
  /**
   * Returns true if the current row is deterministic (i.e., independent of any var terms)
   */
  def deterministicRow(): Boolean;
  /**
   * Returns true if column N of the current row is deterministic (i.e., independent of any var terms).  N = 0 is the first column
   */
  def deterministicCol(v: Int): Boolean;

  /**
   * Returns the number of missing rows encountered **so far**.  In general, this method should only be called once the entire expression
   * has been fully evaluated.
   */
  def missingRows(): Boolean;

  /**
   * Return the schema of the given expression
   */
  def schema: List[(String,Type)];
  
  /**
   * Return the number of columns (i.e., iterator.schema().size())
   */
  def numCols: Int;

  /**
   * Return the current row as a list
   */
  def currentRow(): List[PrimitiveValue] =
    (0 until numCols).map( this(_) ).toList

  def currentTuple(): Map[String, PrimitiveValue] =
    schema.map(_._1).zip(currentRow).toMap

  /**
   * Shorthand foreach operator
   */
  def foreachRow(fn: ResultIterator => Unit): Unit = {
    open()
    while(getNext()){ fn(this) }
    close()
  }

  /**
   * Shorthand map operator over the rows.
   */
  def mapRows[X](fn: ResultIterator => X): List[X] =
  {
    var ret = List[X]()
    foreachRow( (x) => { ret = fn(this) :: ret } )
    ret.reverse;
  }

  /**
   * A list of lists containing all rows remaining in the iterator.
   * Note that this operation exhausts the iterator: Calling this twice
   * will not work.
   */
  def allRows(): List[List[PrimitiveValue]] = 
    mapRows(_.currentRow())

  /**
   * A list of explanations for the indicated column
   */
  def reason(ind: Int): List[Reason] = List()

  /**
   * A unique identifier for every output that can be unwrapped to generate per-row provenance
   */
  def provenanceToken(): RowIdPrimitive
}
