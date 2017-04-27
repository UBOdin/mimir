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
  val annotations = 
    Set( ViewAnnotation.TAINT, ViewAnnotation.PROVENANCE )

  def operator: Operator =
    View(name, query, Set())

  def table: Operator = {
    val baseTable = 
      Project(
        schema.map { case (col, _) => ProjectArg(col, Var(col)) } ++ 
        schema.zipWithIndex.map { case ((col, _), idx) => 
          ProjectArg(
            CTPercolator.mimirColDeterministicColumnPrefix+col,
            Comparison(Cmp.Eq,
              Arithmetic(Arith.BitAnd,
                Var(ViewManager.taintBitVectorColumn),
                IntPrimitive(1l << (idx+1))
              )
            )
          )
        }++ 
        Seq(
          ProjectArg(
            CTPercolator.mimirRowDeterministicColumnName,
            Comparison(Cmp.Eq,
              Arithmetic(Arith.BitAnd,
                Var(ViewManager.taintBitVectorColumn),
                IntPrimitive(1l)
              )
            )
          ),
          ProjectArg(ViewAnnotation.PROVENANCE, Var(ViewAnnotation.PROVENANCE))
        ),
        Table(name, 
          schema ++ 
          if(annotations(ViewAnnotation.TAINT)){
            (ViewManager.taintBitVectorColumn, TInt())
          } else { None } ++ 
          if(annotations(ViewAnnotation.PROVENANCE)){
            Provenance.compile(query)._2.map { (_, TRowId()) }
          }
        )
      )

  }
    Project(

    Table(name, schemaWith(annotations - ViewAnnotation.Provenance)

      ,Seq())

  def schema =
    query.schema

  def schemaWith(requiredAnnotations:Set[ViewAnnotation.T]) =
  {
    schema ++ (
      if(requiredAnnotations(ViewAnnotation.TAINT)) {
        schema.map { col => 
          (CTPercolator.mimirColDeterministicColumnPrefix + col._1, TBool())
        }++
        Seq((CTPercolator.mimirRowDeterministicColumnName, TBool()))
      } else { Seq() }
    ) ++ (
      if(requiredAnnotations(ViewAnnotation.PROVENANCE)) {
        Provenance.compile(query)._2.map { (_, TRowId()) }
      } else { Seq() }
    )
  }

  def fullSchema =
    schemaWith( annotations )
      
}

object ViewAnnotation
  extends Enumeration
{
  type T = Value
  val BEST_GUESS, TAINT, PROVENANCE = Value
}

