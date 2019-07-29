package mimir.views

import mimir.Database
import mimir.algebra._
import mimir.ctables.OperatorDeterminism
import mimir.provenance.Provenance

class ViewMetadata(
  val name: ID,
  val query: Operator,
  val isMaterialized: Boolean,
  db: Database
)
{
  val annotations = 
    Set( ViewAnnotation.TAINT, ViewAnnotation.PROVENANCE )

  def operator: Operator =
    View(name, query, Set())

  def provenanceCols: Seq[ID] = 
    Provenance.compile(query)._2

  def schema: Seq[(ID, Type)] =
  {
    //XXX: No More HACK!  Type Inference has become an adaptive schema
    db.typechecker.schemaOf(query)
  }

  def schemaWith(requiredAnnotations:Set[ViewAnnotation.T]): Seq[(ID, Type)] =
  {
    val sch = schema
    val prov = Provenance.compile(query)._2.map { (_, TRowId()) }
    
    sch ++ (
      if(requiredAnnotations(ViewAnnotation.TAINT)) {
        (sch++prov).map { col => 
          (OperatorDeterminism.mimirColDeterministicColumn(col._1), TBool())
        }++
        Seq((OperatorDeterminism.mimirRowDeterministicColumnName, TBool()))
      } else { None }
    ) ++ (
      if(requiredAnnotations(ViewAnnotation.TAINT_BITS)){
         Seq( (ViewAnnotation.taintBitVectorColumn, TInt()))
      } else { None }
    ) ++ (
      if(requiredAnnotations(ViewAnnotation.PROVENANCE)) {
        prov
      } else { None }
    )
  }

  def materializedName = 
    ID(name, "_MATERIALIZED")

  def materializedSchema =
    schemaWith(annotations.map { case ViewAnnotation.TAINT => ViewAnnotation.TAINT_BITS; case x => x })

  def materializedOperator =
    Table(
      materializedName,
      db.catalog.materializedTableProviderID,
      materializedSchema,
      Seq()
    )

  def fullSchema =
    schemaWith( annotations )
      
}

object ViewAnnotation
  extends Enumeration
{
  type T = Value
  val BEST_GUESS, TAINT, TAINT_BITS, PROVENANCE, SAMPLES, OTHER = Value
  val taintBitVectorColumn = ID("MIMIR_DET_BIT_VECTOR")
}

