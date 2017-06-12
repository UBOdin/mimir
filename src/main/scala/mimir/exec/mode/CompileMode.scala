package mimir.exec.mode

import mimir.Database
import mimir.optimizer._
import mimir.algebra._
import mimir.ctables._
import mimir.provenance._
import mimir.exec._
import mimir.exec.result._
import com.typesafe.scalalogging.slf4j.LazyLogging

/**
 * Base functionality for different compiler rewrites.
 * 
 */
abstract class CompileMode[IteratorT <: ResultIterator]
  extends LazyLogging
{
  type MetadataT

  /**
   * Rewrite the specified operator
   */
  def rewrite(db: Database, oper: Operator): (Operator, Seq[String], MetadataT)

  /**
   * Wrap a resultset generated for the specified operator with a 
   * specific type of resultIterator.
   */
  def wrap(db: Database, results: ResultIterator, query: Operator, meta: MetadataT): IteratorT


  def apply(db: Database, oper: Operator, opts: Compiler.Optimizations = Compiler.standardOptimizations): IteratorT =
  {
    val (rewritten, relevantColumnNames, meta) =
      rewrite(db, oper)

    val results = 
      db.compiler.deploy(rewritten, relevantColumnNames, opts)

    wrap(db, results, rewritten, meta)
  }
}