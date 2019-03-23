package mimir.statistics.facet

import play.api.libs.json._
import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir.Database
import mimir.algebra._


class NonNullable(column: ID)
  extends Facet
{
  def description = s"$column has no nulls"
  def test(db: Database, query: Operator): Seq[String] =
  {
    if(!(query.columnNames contains column)){ return Seq() }
    db.query(
      query.filter { Var(column).isNull }
           .count(alias = "COUNT")
    ) { result =>
      val row = result.next
      val numNulls = row(ID("COUNT")).asLong
      if(numNulls > 0){
        Seq(s"$column had no nulls before, but now has $numNulls")
      } else {
        Seq[String]()
      }
    }
  }

  def toJson: JsValue = JsObject(Map[String,JsValue](
    "facet" -> JsString("NON_NULLABLE"),
    "data"  -> JsString(column.id)
  ))
}

class PctNullable(column: ID, pctExpected: Double)
  extends Facet
{
  def description = s"$column has no more than ${(pctExpected * 1000).toInt / 10.0}% nulls"
  def test(db: Database, query: Operator): Seq[String] =
  {
    if(!(query.columnNames contains column)){ return Seq() }
    db.query(
      query.pctIf( alias = "FRACTION" ) { Var(column).isNull }
    ) { result =>
      val row = result.next
      val pctActual = row(ID("FRACTION")).asDouble
      if(pctActual > pctExpected * 1.1){ // add a small buffer
        Seq(s"$column had only ${(pctExpected * 1000).toInt / 10.0}% nulls before, but now has ${(pctActual * 1000).toInt / 10.0}%")
      } else {
        Seq[String]()
      }
    }
  }

  def toJson: JsValue = JsObject(Map[String,JsValue](
    "facet" -> JsString("PCT_NULLABLE"),
    "data"  -> JsObject(Map[String,JsValue](
                  "column" -> JsString(column.id),
                  "expected" -> JsNumber(pctExpected)
                ))
  ))
}


object Nullable
  extends FacetDetector
  with LazyLogging
{

  def apply(db: Database, query: Operator): Seq[Facet] =
  {
    db.query(
      query.aggregate(
        query.columnNames.map { column =>
          Seq(
            AggFunction(
              ID("avg"),
              false, 
              Seq( 
                Var(column).isNull
                           .thenElse { FloatPrimitive(1) }
                                     { FloatPrimitive(0) }
              ),
              ID("AVG_",column)
            ),
            AggFunction(
              ID("sum"),
              false, 
              Seq( 
                Var(column).isNull
                           .thenElse { IntPrimitive(1) }
                                     { IntPrimitive(0) }
              ),
              ID("COUNT_",column)
            )
          )
        }.flatten:_*
      )
    ) { result => 
      val row = result.next
      logger.info(s"Test found: $row")
      query.columnNames.map { column => 
        if(row(ID("COUNT_",column)).asLong == 0){
          Some(new NonNullable(column))
        } else if(row(ID("AVG_",column)).asDouble < 0.9){
          Some(new PctNullable(column, row(ID("AVG_"+column)).asDouble))
        } else { None }
      }.flatten
    }
  }

  def jsonToFacet(body: JsValue): Option[Facet] = {
    body match { 
      case JsObject(fields) => {
        fields.get("facet") match {
          case Some(JsString("NON_NULLABLE")) => 
            Some(new NonNullable(ID(fields("data").as[JsString].value)))
          case Some(JsString("PCT_NULLABLE")) => {
            val data = fields("data").as[JsObject].value
            Some(new PctNullable(
              ID(data("column").as[JsString].value), 
              data("expected").as[JsNumber].value.toDouble
            ))
          }
          case _ => None
        }
      }
      case _ => None
    }
  }
}
