package mimir.statistics.facet

import mimir.Database
import mimir.algebra._

class ExpectedColumns(expected: Seq[String])
  extends Facet
{
  def description = s"The dataset includes columns: ${expected.mkString(", ")}"
  def test(db:Database, query:Operator): Seq[String] =
  {
    val actual = query.columnNames
    if(expected.toSet == actual.toSet){
      if(expected.size != actual.size){
        if(actual.size < actual.toSet.size) {
          Seq(s"Duplicated columns missing in ${actual.mkString(", ")} (Expected: ${expected.mkString(", ")})")
        } else { 
          Seq(s"Unexpected duplicated columns in ${actual.mkString(", ")} (Expected: ${expected.mkString(", ")})")
        }
      } else {
        if(expected.zip(actual).exists { case (e, a) => !e.equals(a) }){
          Seq(s"Columns out of order: Got ${actual.mkString(", ")} but expected: ${expected.mkString(", ")}")
        } else {
          Seq() // all is well
        }
      }
    } else {
      (expected.toSet &~ actual.toSet).toSeq.map { 
        "Missing expected column '"+_+"'"
      } ++ 
      (actual.toSet &~ expected.toSet).toSeq.map { 
        "Unexpected column '"+_+"'"
      }
    }
  }
}

object ExpectedColumns
{
  def apply(db:Database, query:Operator): Seq[Facet] = 
    Seq(new ExpectedColumns(query.columnNames))
}