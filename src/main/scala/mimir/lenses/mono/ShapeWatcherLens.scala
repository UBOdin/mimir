package mimir.lenses.mono

import java.sql.SQLException
import play.api.libs.json._

import mimir.Database
import mimir.algebra._
import mimir.ctables.Reason
import mimir.lenses._
import mimir.statistics.DatasetShape
import mimir.statistics.facet.{ Facet, AppliesToColumn, AppliesToRow }

object ShapeWatcherLens extends MonoLens
{
  def train(
    db: Database,
    name: ID,
    query: Operator,
    config: JsValue
  ): JsValue = 
  {
    config match {
      case JsNull => JsArray(DatasetShape.detect(db, query).map { _.toJson })
      case JsArray(_) => {
        //validate the config
        val parsed = config.as[Seq[Facet]]
        config
      }
      case _ => throw new SQLException(s"Invalid lens configuration $config")
    }
  }

  def view(
    db: Database,
    name: ID,
    baseQuery: Operator,
    config: JsValue,
    friendlyName: String
  ): Operator = 
  {
    config.as[Seq[Facet]]
          .foldLeft(baseQuery) { (query, facet) => 
            facet match {
              case f:AppliesToColumn => 
                query.alterColumnsByID(
                  f.appliesToColumn -> 
                    f.facetInvalidCondition
                     .thenElse { 
                        Caveat(
                          name, 
                          Var(f.appliesToColumn), 
                          Seq(Var(f.appliesToColumn)), 
                          f.facetInvalidDescription
                        )
                     } { Var(f.appliesToColumn) }
                )
              case f:AppliesToRow =>
                query.filter { 
                  f.facetInvalidCondition
                   .thenElse { 
                      Caveat(
                        name, 
                        BoolPrimitive(true), 
                        Seq(RowIdVar()), 
                        f.facetInvalidDescription
                      )
                   } { BoolPrimitive(true) }

                }
              case _ => query
            }
          }
  }

  def warnings(
    db: Database, 
    name: ID, 
    query: Operator, 
    cols: Seq[ID],
    config: JsValue, 
    friendlyName: String
  ): Seq[Reason] =
  {
    config.as[Seq[Facet]]
          .flatMap { 
            case _:AppliesToColumn | _:AppliesToRow => None
            case genericFacet => genericFacet.test(db, query) 
          }
          .map { Reason(name, Seq(), _, false) }
  }
}
