package mimir.statistics.facet

import play.api.libs.json._
import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir.Database
import mimir.algebra._
import mimir.util.StringUtils

class EnumDomain(column: String, values: Seq[String])
  extends Facet
{
  def description = s"$column should be one of ${StringUtils.nMore(5, values, conjunction = "or")}"
  def test(db: Database, query: Operator): Seq[String] =
  {
    if(!(query.columnNames contains column)){ return Seq() }
    val colType = db.typechecker.schemaOf(query.project(column))(0)._2
    if(Type.rootType(colType) != TString()){ return Seq() }

    db.query(
      query.project(column)
           .distinct
           .filter { ExpressionUtils.makeNot(
                        Var(column).isNull.or(
                          Var(column).in(values.map { StringPrimitive(_) })
                        )
                      ) }
           
    ) { result =>
      val outOfBoundsValues = result.map { _(column).asString }.toSeq
      if(outOfBoundsValues.isEmpty){ Seq() }
      else {
        Seq(s"$column contained unexpected values: ${StringUtils.nMore(3, outOfBoundsValues, conjunction = "or")}")
      }
    }
  }

  def toJson: JsValue = JsObject(Map[String,JsValue](
    "facet" -> JsString("ENUM_DOMAIN"),
    "data"  -> JsObject(Map[String, JsValue](
                "column" -> JsString(column),
                "values" -> JsArray(values.map { JsString(_) })
              ))
  ))
}

object Domain
  extends FacetDetector
  with LazyLogging
{
  val THRESHOLD = 100

  def apply(db: Database, query: Operator): Seq[Facet] =
  {
    val stringColumns = 
      db.typechecker.schemaOf(query)
        .filter { col => Type.rootType(col._2) == TString() }
        .map { _._1 }

    val enumColumns =
      db.query(
        query.aggregate(
          stringColumns.map { column => 
            AggFunction(
              "COUNT",
              true,
              Seq(Var(column)),
              "STR_"+column
            )
          }:_*
        )
      ) { result => 
        val row = result.next
        logger.info(s"Test found: $row")
        stringColumns.filter { column => 
          row("STR_"+column).asLong < THRESHOLD
        }
      }

    enumColumns.map { column => 
      val validValues =
        db.query(
          query.project(column)
               .filter { Var(column).isNull.not }
               .distinct
        ) { result => 
          result.map { _(column).asString }
        }

      new EnumDomain(column, validValues.toSeq)
    }
  }

  def jsonToFacet(body: JsValue): Option[Facet] = {
    body match { 
      case JsObject(fields) => {
        fields.get("facet") match {
          case Some(JsString("ENUM_DOMAIN")) => {
            val data = fields("data").as[JsObject].value
            Some(new EnumDomain(
              data("column").as[JsString].value, 
              data("values").as[JsArray].value.map { _.as[JsString].value }
            ))
          }
          case _ => None
        }
      }
      case _ => None
    }
  }
}
