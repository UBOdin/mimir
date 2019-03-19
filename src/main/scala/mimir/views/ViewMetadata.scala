package mimir.views

import mimir.Database
import mimir.algebra._
import mimir.ctables.CTPercolator
import mimir.provenance.Provenance

class ViewMetadata(
  val name: Name,
  val query: Operator,
  val isMaterialized: Boolean,
  db: Database
)
{
  val annotations = 
    Set( ViewAnnotation.TAINT, ViewAnnotation.PROVENANCE )

  def operator: Operator =
    View(name, query, Set())

  def table: Operator = {
    Project(
      schema.map { case (col, _) => ProjectArg(col, Var(col)) } ++ 
      schemaWith(Set(ViewAnnotation.PROVENANCE)).zipWithIndex.map { case ((col, _), idx) => 
        ProjectArg(
          CTPercolator.mimirColDeterministicColumnPrefix+col,
          Comparison(Cmp.Eq,
            Arithmetic(Arith.BitAnd,
              Var(ViewAnnotation.taintBitVectorColumn),
              IntPrimitive(1l << (idx+1))
            ),
            IntPrimitive(1l << (idx+1))
          )
        )
      }++ 
      (if(annotations(ViewAnnotation.TAINT)){
        Seq(
          ProjectArg(
            CTPercolator.mimirRowDeterministicColumnName,
            Comparison(Cmp.Eq,
              Arithmetic(Arith.BitAnd,
                Var(ViewAnnotation.taintBitVectorColumn),
                IntPrimitive(1l)
              ),
              IntPrimitive(1l)
            )
          )
        )
      } else { None })++
      (if(annotations(ViewAnnotation.PROVENANCE)){
        provenanceCols.map { col => ProjectArg(col, Var(col)) }
      } else { None }),
      Table(name, name, materializedSchema, Seq())
    )
  }
  
  def materializedOperator: Operator =  {
    Project(materializedSchema.map {col => ProjectArg(col._1, Var(col._1))},  
        Table(name, name, materializedSchema, Seq()))
  }

  def provenanceCols: Seq[String] = 
    Provenance.compile(query)._2

  def schema: Seq[(Name, Type)] =
  {
    //XXX: No More HACK!  Type Inference has become an adaptive schema
    db.typechecker.schemaOf(query)
  }

  def schemaWith(requiredAnnotations:Set[ViewAnnotation.T]) =
  {
    val sch = schema
    val prov = Provenance.compile(query)._2.map { (_, TRowId()) }
    
    sch ++ (
      if(requiredAnnotations(ViewAnnotation.TAINT)) {
        (sch++prov).map { col => 
          (CTPercolator.mimirColDeterministicColumnPrefix + col._1, TBool())
        }++
        Seq((CTPercolator.mimirRowDeterministicColumnName, TBool()))
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

  def materializedSchema =
    schemaWith(annotations.map { case ViewAnnotation.TAINT => ViewAnnotation.TAINT_BITS; case x => x })

  def fullSchema =
    schemaWith( annotations )
      
}

object ViewAnnotation
  extends Enumeration
{
  type T = Value
  val BEST_GUESS, TAINT, TAINT_BITS, PROVENANCE, SAMPLES, OTHER = Value
  val taintBitVectorColumn = "MIMIR_DET_BIT_VECTOR"
}

