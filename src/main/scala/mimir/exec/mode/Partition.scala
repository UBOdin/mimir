package mimir.exec.mode

import mimir.Database
import mimir.algebra._
import mimir.exec.result.ResultIterator

object Partition
  extends CompileMode[ResultIterator]
{

  type MetadataT = Unit

  def rewrite(db: Database, oper: Operator): (Operator, Seq[String], MetadataT) = ???
  def wrap(db: Database, results: ResultIterator, query: Operator, meta: MetadataT): ResultIterator = ???
}