package mimir.statistics.facet

import play.api.libs.json._
import mimir.Database
import mimir.algebra._
import mimir.util.StringUtils
import mimir.serialization.Json

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
  def toJson: JsValue = JsObject(Map[String,JsValue](
    "facet" -> JsString("ExpectedType"),
    "data"  -> JsObject(Map[String,JsValue](
      "column" -> JsString(column),
      "expected" -> Json.ofType(tExpected)
    ))
  ))
}

object ExpectedType
  extends FacetDetector
{
  def apply(db:Database, query:Operator): Seq[Facet] = 
    db.typechecker.schemaOf(query).map { 
      case (column, tExpected) => new ExpectedType(column, tExpected)
    }
  def jsonToFacet(data: JsValue): Option[Facet] = {
    data match { 
      case JsObject(fields) if fields.get("facet").equals(Some(JsString("ExpectedType"))) => {
        Some(
          new ExpectedType(
            fields("column").asInstanceOf[JsString].value,
            Json.toType(fields("expected"))
          )
        )
      }
      case _ => None
    }
  }
}