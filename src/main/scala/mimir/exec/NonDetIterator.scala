package mimir.exec;

import java.sql._;

import mimir.algebra._;
import mimir.ctables.Reason;

class NonDetIterator(val src: ResultIterator, 
                     val schema: Seq[(String, Type)],
                     colDeterminism: Seq[Expression], 
                     rowDeterminism: Expression) 
  extends ResultIterator
{
  val compiledColDeterminism = colDeterminism.map(VarProjection.compile(src, _))
  val compiledRowDeterminism = VarProjection.compile(src, rowDeterminism)

  def apply(v: Int): PrimitiveValue = src(v)

  def deterministicRow(): Boolean = 
    Eval.evalBool(compiledRowDeterminism)
  def deterministicCol(v: Int): Boolean =
    Eval.evalBool(compiledColDeterminism(v))
  def missingRows(): Boolean = { 
    System.err.println("Warning: NonDetIterator.missingRows unimplemented"); 
    true
  }
  def open()             = { src.open() }
  def getNext(): Boolean = { src.getNext() }
  def close()            = { src.close() }
  def numCols: Int       = { schema.length }

  def provenanceToken() = src.provenanceToken()
}