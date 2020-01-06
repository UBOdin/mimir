package mimir.adaptive

import mimir.Database
import mimir.models._
import mimir.algebra._
import mimir.statistics.DatasetShape

object ShapeWatcher
  extends Multilens
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
        val facet = DatasetShape.parse(row(ID("FACET")).asString)
        val facet_id = row(ID("ID"))
        // First check if the facet is applicable.
        val warnings = facet.test(db, config.query)

        // It's not elegant, but use a selection predicate to 
        // annotate the row.  The predicate should get compiled away
        // but the data warning will be applied to the entire row.
        for(warning <- warnings){
          base = base.filter { 
            DataWarning(
              ID(s"$modelName:$facet_id"), 
              BoolPrimitive(true), 
              StringPrimitive(warning), 
              Seq(row(ID("ID")), StringPrimitive(warning))
            )
          }
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
      Some(config.query)
    } else {
      None
    }
}

