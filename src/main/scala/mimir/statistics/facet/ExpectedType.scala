package mimir.statistics.facet

import mimir.Database
import mimir.algebra._
import mimir.util.StringUtils

class ExpectedType(column: String, tExpected: Type)
  extends Facet
{
  def description = s"The column ${column} should be ${StringUtils.withDefiniteArticle(tExpected.toString)}"
  def test(db:Database, query:Operator): Seq[String] =
  {
    db.typechecker.schemaOf(query)
      .filter { _._1.toUpperCase == column.toUpperCase }
      // silently pass through missing columns.  Should be caught by ExpectedColumns
      .flatMap { 
        case (_, tActual) => 
          if(tExpected != tActual) { 
            Some(s"${column} is ${StringUtils.withDefiniteArticle(tActual.toString)} (Expected ${StringUtils.withDefiniteArticle(tExpected.toString)})") 
          } else { None }
      }
  }
}

object ExpectedType
{
  def apply(db:Database, query:Operator): Seq[Facet] = 
    db.typechecker.schemaOf(query).map { 
      case (column, tExpected) => new ExpectedType(column, tExpected)
    }
}