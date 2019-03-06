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
    val warningModel = (modelName:String) => db.models.getOption(modelName) match {
          case Some(model) => model
          case None => createWarningModel(db, config, modelName)
        }
    val model = config.args match {
      case Seq() => {
        createWarningModel(db, config, "MIMIR_SHAPE_"+config.schema)
      }
      case Seq(StringPrimitive(modelName)) => warningModel(modelName)
      case Seq(Var(modelName)) =>  warningModel(modelName)
    }
    
    return Seq(
      model
    )
  }
  
  private def createWarningModel(db: Database, config: MultilensConfig, modelName:String) = {
    val facets = DatasetShape.detect(db, config.query)
    val facetTable = modelName
    val facetTableData = HardTable(
      Seq( "ID" -> TInt(), "FACET" -> TString() ),
      facets.zipWithIndex
            .map { case (facet, id) => 
              Seq(IntPrimitive(id), StringPrimitive(facet.toJson.toString))
            }
    )
    db.views.create(facetTable, facetTableData)
    WarningModel(modelName, Seq(TInt(), TString()))
  }
  
  def tableCatalogFor(db: Database, config: MultilensConfig): Operator = 
  {
    val modelName = config.args match {
      case Seq() =>  "MIMIR_SHAPE_"+config.schema
      case Seq(Var(modelN)) => modelN
      case Seq(StringPrimitive(modelN)) => modelN
    }
    val facetTable = modelName
    var base:Operator = HardTable(
      Seq("TABLE_NAME" -> TString()),
      Seq(Seq(StringPrimitive(config.schema.toUpperCase)))
    )
    db.query(db.table(facetTable)) { result =>
      for(row <- result){ 
        val facet = DatasetShape.parse(row("FACET").asString)

        // First check if the facet is applicable.
        val warnings = facet.test(db, config.query)

        // It's not elegant, but use a selection predicate to 
        // annotate the row.  The predicate should get compiled away
        // but the data warning will be applied to the entire row.
        for(warning <- warnings){
          base = base.filter { 
            DataWarning(
              modelName, 
              BoolPrimitive(true), 
              StringPrimitive(warning), 
              Seq(row("ID"), StringPrimitive(warning))
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
        "TABLE_NAME" -> TString(), 
        "ATTR_NAME"  -> TString(),
        "IS_KEY"     -> TBool(),
        "ATTR_TYPE"  -> TType()
      ),
      db.typechecker.schemaOf(config.query).map { case (col, t) =>
        Seq(
          StringPrimitive(config.schema),  //Table Name
          StringPrimitive(col),            //Column Name
          BoolPrimitive(false),            //Is Key?
          TypePrimitive(t)                 //Column Type
        )
      }
    )
  }

  def viewFor(db: Database, config: MultilensConfig, table: String): Option[Operator] = 
    if(table.toLowerCase.equals(config.schema.toLowerCase)){
      Some(config.query)
    } else {
      None
    }
}

