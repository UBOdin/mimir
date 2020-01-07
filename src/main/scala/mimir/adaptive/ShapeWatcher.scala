package mimir.adaptive

import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir.Database
import mimir.models._
import mimir.algebra._
import mimir.statistics.DatasetShape
import mimir.statistics.facet.{ Facet, AppliesToRow, AppliesToColumn }


object ShapeWatcher
  extends Multilens
  with LazyLogging
{
  def initSchema(db: Database, config: MultilensConfig): TraversableOnce[Model] = 
  {
    val warningModel = (modelName:ID) => 
      db.views.get(modelName) match {
        case None => createWarningModels(db, config, modelName)
        case Some(meta) => 
          db.query(meta.operator) { results =>
            results.map { row => 
              val idx = row(ID("ID"))
              db.models.get(ID(s"$modelName:$idx"))
            }.toIndexedSeq
          }
      }
    val models = config.args match {
      case Seq() => {
        warningModel(ID("MIMIR_SHAPE_", config.schema))
      }
      case Seq(StringPrimitive(modelName)) => warningModel(ID(modelName))
      case Seq(Var(modelName)) =>  warningModel(modelName)
    }
    
    return models
  }
  
  private def createWarningModels(db: Database, config: MultilensConfig, modelName:ID): Seq[WarningModel] = {
    val facets = DatasetShape.detect(db, config.query)
    val facetTable = modelName
    val facetsWithIndex = facets.zipWithIndex
    val facetTableData = HardTable(
      Seq( ID("ID") -> TInt(), ID("FACET") -> TString() ),
      facetsWithIndex
            .map { case (facet, id) => 
              Seq(IntPrimitive(id), StringPrimitive(facet.toJson.toString))
            }
    )
    db.views.create(facetTable, facetTableData)
    facetsWithIndex.map { case (_, id) => 
      WarningModel(ID(s"${modelName.id}:$id"), Seq(TInt(), TString()))
    }
  }

  def annotate(
    db: Database, 
    config:MultilensConfig, 
    name: ID, 
    expr: Expression, 
    warnings: Seq[Expression],
    id: Seq[Expression] = Seq()
  ): Expression =
  { 
    // It's not elegant, but use a selection predicate to 
    // annotate the row.  The predicate should get compiled away
    // but the data warning will be applied to the entire row.
    warnings
      .zipWithIndex
      .foldLeft(expr) { 
        (oldExpr, warning) => 
          DataWarning(
            name,
            oldExpr, 
            warning._1, 
            IntPrimitive(warning._2) +: id
          )
      }
  }

  def tableCatalogFor(db: Database, config: MultilensConfig): Operator = 
  {
    val modelName:ID = config.args match {
      case Seq() =>  ID("MIMIR_SHAPE_", config.schema)
      case Seq(Var(modelN)) => modelN
      case Seq(StringPrimitive(modelN)) => ID(modelN)
    }
    val facetTable = modelName
    var base:Operator = HardTable(
      Seq( ID("TABLE_NAME") -> TString()),
      Seq(Seq(StringPrimitive(config.schema.id)))
    )
    db.query(db.catalog.tableOperator(facetTable)) { result =>
      for(row <- result){ 
        DatasetShape.parse(row(ID("FACET")).asString) match {
          case _:AppliesToColumn => {}
          case _:AppliesToRow => {}
          case facet:Facet => 
            annotate(
              db, config,
              ID(s"$modelName:${row(ID("ID"))}"), 
              BoolPrimitive(true),
              facet.test(db, config.query)
                .map { StringPrimitive(_) }
            ) 
        }
      }
    }
    return base
  }
  def attrCatalogFor(db: Database, config: MultilensConfig): Operator = 
  {
    HardTable(
      Seq( 
        ID("TABLE_NAME") -> TString(), 
        ID("ATTR_NAME")  -> TString(),
        ID("IS_KEY")     -> TBool(),
        ID("ATTR_TYPE")  -> TType()
      ),
      db.typechecker.schemaOf(config.query).map { case (col, t) =>
        Seq(
          StringPrimitive(config.schema.id),  //Table Name
          StringPrimitive(col.id),            //Column Name
          BoolPrimitive(false),               //Is Key?
          TypePrimitive(t)                    //Column Type
        )
      }
    )
  }

  def viewFor(db: Database, config: MultilensConfig, table: ID): Option[Operator] = 
    if(table.equals(config.schema)){
      val modelName:ID = config.args match {
        case Seq() =>  ID("MIMIR_SHAPE_", config.schema)
        case Seq(Var(modelN)) => modelN
        case Seq(StringPrimitive(modelN)) => ID(modelN)
      }
      val facetTable = modelName
      var query = config.query
      db.query(db.catalog.tableOperator(facetTable)) { result =>
        for(row <- result){ 
          DatasetShape.parse(row(ID("FACET")).asString) match {
            case facet:AppliesToColumn => 
              query = query.alterColumnsByID(
                facet.appliesToColumn -> 
                  facet.facetInvalidCondition
                       .thenElse { 
                          annotate(
                            db, config,
                            ID(s"$modelName:${row(ID("ID"))}"), 
                            Var(facet.appliesToColumn),
                            Seq(facet.facetInvalidDescription),
                            Seq(RowIdVar())
                          )
                        } { Var(facet.appliesToColumn) }
              )
            case facet:AppliesToRow => 
              query = query.filter { 
                  facet.facetInvalidCondition
                       .thenElse { 
                          annotate(
                            db, config,
                            ID(s"$modelName:${row(ID("ID"))}"), 
                            BoolPrimitive(true),
                            Seq(facet.facetInvalidDescription),
                            Seq(RowIdVar())
                          ) 
                        } { BoolPrimitive(true) }
              } 
            case _:Facet => {}
          }
        }
      }
      logger.debug(s"ShapeWatcher ${config.schema}: $query")
      return Some(query)
    } else {
      None
    }
}









