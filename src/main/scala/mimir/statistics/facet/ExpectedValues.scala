package mimir.statistics.facet

import play.api.libs.json._
import com.typesafe.scalalogging.LazyLogging

import mimir.Database
import mimir.algebra._
import mimir.util.StringUtils
import mimir.serialization.Json


class DrawnFromDomain(var column: ID, var t: Type, var domain: Set[PrimitiveValue])
  extends Facet
  with AppliesToColumn
  with LazyLogging
{
  def validAssignmentString(conjunct:String) = 
    StringUtils.oxfordComma(domain.map { _.toString }.toSeq, conjunct)
  def description = 
    s"$column must be one of ${validAssignmentString("or")}"

  def appliesToColumn = column
  def facetInvalidCondition = 
    ExpressionUtils.makeAnd(domain.map { Var(column).neq(_) })
  def facetInvalidDescription =
    Function(ID("concat"), Seq(
      StringPrimitive(s"$description, but found "),
      Var(column),
      StringPrimitive(" instead.")
    ))


  def test(db: Database, query: Operator): Seq[String] =
  {
    if(!query.columnNames.contains(column)){ return Seq() }
    
    logger.debug(s"Invalid lookup query: $facetInvalidCondition")
    db.query(
      query.filter { facetInvalidCondition }
           .groupBy(Var(column))(
              AggFunction(ID("count"), false, Seq(), ID("TOT"))
           )
    ) { result => 
      val errorValues = 
        result.map { row => s"${row(0)} (${row(1)} times)" }
              .toIndexedSeq
      logger.debug(errorValues.mkString(", "))
      if(errorValues.size > 0){
        val trimmedErrorValues = 
          if(errorValues.size > 5){ 
            errorValues.take(5) ++ Seq(s"${errorValues.size-5} more")
          } else { errorValues }
        val trimmedErrorString = 
          StringUtils.oxfordComma(trimmedErrorValues, "and")

        Seq(
          (
            s"Previously $column only contained ${validAssignmentString("and")}, "+
            s"but now also contains $trimmedErrorString"
          )
        )
      } else { Seq() }
    }
  }

  def toJson: JsValue = JsObject(Map[String,JsValue](
    "facet" -> JsString("DRAWN_FROM_DOMAIN"),
    "data"  -> JsObject(Map[String,JsValue](
      "column" -> JsString(column.id),
      "domain" -> JsArray(
        domain.toSeq.map { Json.ofPrimitive(_) }
      ),
      "type"   -> Json.ofType(t)
    ))
  ))

  override def equals(other: Any): Boolean =
  { 
    other match { 
      case n:DrawnFromDomain => 
        n.column.equals(column) &&
        n.t.equals(t) &&
        n.domain.equals(domain)
      case _ => false
    }
  }
}

class DrawnFromRange(column: ID, t: Type, low:PrimitiveValue, high:PrimitiveValue)
  extends Facet
  with AppliesToColumn
{
  def description = 
    s"$column must be between $low and $high"

  def appliesToColumn = column
  def facetInvalidCondition = 
    ExpressionUtils.makeOr(Seq(
      Var(column).lt(low),
      Var(column).gt(high)
    ))
  def facetInvalidDescription =
    Function(ID("concat"), Seq(
      StringPrimitive(s"$description, but found "),
      Var(column),
      StringPrimitive(" instead.")
    ))

  def test(db: Database, query: Operator): Seq[String] =
  {
    if(!query.columnNames.contains(column)){ return Seq() }
    
    db.query(
      query.filter { facetInvalidCondition }
           .aggregate(
              AggFunction(ID("count"), false, Seq(), ID("cnt")),
              AggFunction(ID("min"), false, Seq(Var(column)), ID("low")),
              AggFunction(ID("max"), false, Seq(Var(column)), ID("high"))
           )
    ) { result => 
      val row = result.next()
      if(row(0).asLong > 0){
        val newValueSummary = 
          if(row(1).equals(row(2))) {
            row(1).toString
          } else {
            s"values between ${row(1)} and ${row(2)}"
          }

        Seq(
          (
            s"Previously $column only contained values between $low and $high "+
            s"but now also contains $newValueSummary (${row(0)} unexpected values)"
          )
        )
      } else { Seq() }
    }
  }

  def toJson: JsValue = JsObject(Map[String,JsValue](
    "facet" -> JsString("DRAWN_FROM_RANGE"),
    "data"  -> JsObject(Map[String,JsValue](
      "column" -> JsString(column.id),
      "low"    -> Json.ofPrimitive(low),
      "high"   -> Json.ofPrimitive(high),
      "type"   -> Json.ofType(t)
    ))
  ))
}

object ExpectedValues
  extends FacetDetector
  with LazyLogging
{
  def apply(db: Database, query: Operator): Seq[Facet] =
  {
    val colTypes = db.typechecker.schemaOf(query)

    val rangeColumns = 
      colTypes.collect { case (a, TInt() | TFloat() | TDate() | TTimestamp()) => a }
    val domainCandidateColumns = 
      colTypes.collect { case (a, TString()) => a }
    val colTypeMap = colTypes.toMap
    
    logger.trace(s"Range: $rangeColumns")
    val rangeFacets =
      db.query(
        query.aggregate(
          rangeColumns.map { col => 
            Seq(
              AggFunction(ID("min"), false, Seq(Var(col)), ID("LOW_"+col.id)),
              AggFunction(ID("max"), false, Seq(Var(col)), ID("HIGH_"+col.id))
            )
          }.flatten:_*
        )
      ) { result => 
        val row = result.next()
        rangeColumns.map { col =>
          new DrawnFromRange(
            col, 
            colTypeMap(col), 
            row(ID("LOW_"+col.id)),
            row(ID("HIGH_"+col.id))
          )
        }
      }
    logger.trace(s"Range: ${rangeFacets.map {_.description}.mkString(", ")}")

    logger.trace(s"Domain Candidates: $domainCandidateColumns")
    val domainColumns = 
      db.query(
        query.aggregate(
          domainCandidateColumns.map { col => 
            AggFunction(ID("count"), true, Seq(Var(col)), col)
          }:_*
        )
      ) { result =>
        val row = result.next()
        domainCandidateColumns.flatMap { col =>
          if(row(col).asLong < 20){ Some(col) } 
          else { None }
        }
      }
    logger.trace(s"Valid Domain Columns: $domainColumns")
    val domainFacets =
      domainColumns.map { col =>
        db.query(
          query.projectByID(col)
               .distinct
        ) { result => 
          new DrawnFromDomain(
            col,
            colTypeMap(col),
            result.map { _(0) }
                  .toIndexedSeq
                  .toSet
          )
        }
      }
    logger.trace(s"Domain: ${domainFacets.map {_.description}.mkString(", ")}")
    
    return rangeFacets ++ domainFacets
  }


  def jsonToFacet(body: JsValue): Option[Facet] = {
    body match { 
      case JsObject(fields) => {
        fields.get("facet") match {
          case Some(JsString("DRAWN_FROM_DOMAIN")) => {
            val data = fields("data").as[JsObject].value
            val t = Json.toType(data("type"))
            Some(new DrawnFromDomain(
              ID(data("column").as[JsString].value),
              t,
              data("domain").as[JsArray].value
                .map { Json.toPrimitive(t, _) }
                .toSet
            ))
          }
          case Some(JsString("DRAWN_FROM_RANGE")) => {
            val data = fields("data").as[JsObject].value
            val t = Json.toType(data("type"))
            Some(new DrawnFromRange(
              ID(data("column").as[JsString].value),
              t,
              Json.toPrimitive(t, data("low")),
              Json.toPrimitive(t, data("high"))
            ))
          }
          case _ => None
        }
      }
      case _ => None
    }
  }
}