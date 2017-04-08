package mimir.views

import mimir.algebra._
import mimir.ctables.CTPercolator
import mimir.provenance.Provenance

class ViewMetadata(
  val name: String,
  val query: Operator,
  val isMaterialized: Boolean
)
{
  def operator: Operator =
    View(name, query, Set())

  def schema =
    query.schema

  def schemaWith(annotations:Set[ViewAnnotation.T]) =
  {
    schema ++ (
      if(annotations(ViewAnnotation.TAINT)) {
        schema.map { col => 
          (CTPercolator.mimirColDeterministicColumnPrefix + col._1, TBool())
        }++
        Seq((CTPercolator.mimirRowDeterministicColumnName, TBool()))
      } else { Seq() }
    ) ++ (
      if(annotations(ViewAnnotation.PROVENANCE)) {
        Provenance.compile(query)._2.map { (_, TRowId()) }
      } else { Seq() }
    )
  }

  def fullSchema =
    schemaWith( Set( ViewAnnotation.TAINT, ViewAnnotation.PROVENANCE ) )
      
}

object ViewAnnotation
  extends Enumeration
{
  type T = Value
  val BEST_GUESS, TAINT, PROVENANCE = Value
}

