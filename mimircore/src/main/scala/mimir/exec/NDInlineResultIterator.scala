package mimir.exec;

import java.sql._;

import mimir.algebra._;

class NDInlineResultIterator(src: ResultIterator, 
                             querySchema: List[(String, Type.T)],
                             colDeterminism: List[Expression], 
                             rowDeterminism: Expression) 
  extends ResultIterator
{
  val schemaMap = {
    val srcMap = src.schema.map(_._1).zipWithIndex.toMap;
    querySchema.map(_._1).map( srcMap(_) )
  }
  val compiledColDeterminism = colDeterminism.map(VarProjection.compile(src, _))
  val compiledRowDeterminism = VarProjection.compile(src, rowDeterminism)

  def apply(v: Int): PrimitiveValue = src(schemaMap(v))

  def deterministicRow(): Boolean = 
    Eval.evalBool(compiledRowDeterminism)
  def deterministicCol(v: Int): Boolean =
    Eval.evalBool(compiledColDeterminism(v))
  def missingRows(): Boolean = { 
    System.err.println("Warning: NDInlineResultIterator.missingRows unimplemented"); 
    true
  }
  def open()             = { src.open() }
  def getNext(): Boolean = { src.getNext() }
  def close()            = { src.close() }
  def numCols: Int       = { querySchema.length }
  def schema: List[(String,Type.T)] = querySchema

  override def reason(v: Int): List[(String, String)] = {
    throw new SQLException("Must call reason on a query compiled in classical mode")
  }
}