package mimir.views

import mimir.algebra._

class ViewMetadata(
  val query: Operator,
  val isMaterialized: Boolean = false,
  val rowId: Option[Seq[String]] = None,
  val taint: Option[(Map[String, Var], Var)] = None
)
{
  def hasRowId = rowId != None
  def hasTaint = taint != None
  def taintColumns: Seq[(String,Type)]  = 
  {
    (taint.get._1.values.toSeq ++ Seq(taint.get._2)).map { col => (col.name, TInt()) }
  }

  def materializationMetadata: Long =
  {
    (
        (if(isMaterialized){ ViewMetadata.IS_MATERIALIZED } else { 0 })
      | (if(hasRowId      ){ ViewMetadata.HAS_ROWID       } else { 0 })
      | (if(hasTaint      ){ ViewMetadata.HAS_TAINT       } else { 0 })
    )
  }
}

object ViewMetadata
{
  val IS_MATERIALIZED    = 1l << 0
  val HAS_ROWID          = 1l << 1
  val HAS_TAINT          = 1l << 2

  def isMaterialized(meta: Long): Boolean = 
    (meta & IS_MATERIALIZED) == IS_MATERIALIZED
  def hasRowId(meta: Long): Boolean = 
    (meta & (IS_MATERIALIZED | HAS_ROWID)) == (IS_MATERIALIZED | HAS_ROWID)
  def hasTaint(meta: Long): Boolean = 
    (meta & (IS_MATERIALIZED | HAS_TAINT)) == (IS_MATERIALIZED | HAS_TAINT)
}