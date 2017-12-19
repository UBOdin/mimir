package mimir.exec.mode

import com.typesafe.scalalogging.slf4j.LazyLogging
import mimir.Database
import mimir.algebra._
import mimir.exec._
import mimir.exec.result._

object UnannotatedBestGuess
  extends CompileMode[ResultIterator]
  with LazyLogging
{
  type MetadataT = Seq[String]

  /**
   * Rewrite the specified operator
   */
  def rewrite(db: Database, oper: Operator): (Operator, Seq[String], MetadataT) =
  {
    (
      BestGuess.bestGuessQuery(db, oper),
      oper.columnNames,
      Seq()
    )
  }

  /**
   * Wrap a resultset generated for the specified operator with a 
   * specific type of resultIterator.
   */
  def wrap(db: Database, results: ResultIterator, query: Operator, meta: MetadataT): ResultIterator =
  { 
    return results;
  }
}