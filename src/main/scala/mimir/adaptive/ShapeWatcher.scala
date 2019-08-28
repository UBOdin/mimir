// package mimir.adaptive

// import mimir.Database
// import mimir.models._
// import mimir.algebra._
// import mimir.statistics.DatasetShape

// object ShapeWatcher
//   extends Multilens
// {
//   def initSchema(db: Database, config: MultilensConfig): TraversableOnce[Model] = 
//   {
//     val warningModel = (modelName:ID) => db.models.getOption(modelName) match {
//           case Some(model) => model
//           case None => createWarningModel(db, config, modelName)
//         }
//     val model = config.args match {
//       case Seq() => {
//         warningModel(ID("MIMIR_SHAPE_", config.schema))
//       }
//       case Seq(StringPrimitive(modelName)) => warningModel(ID(modelName))
//       case Seq(Var(modelName)) =>  warningModel(modelName)
//     }
    
//     return Seq(
//       model
//     )
//   }
  
//   private def createWarningModel(db: Database, config: MultilensConfig, modelName:ID) = {
//     val facets = DatasetShape.detect(db, config.query)
//     val facetTable = modelName
//     val facetTableData = HardTable(
//       Seq( ID("ID") -> TInt(), ID("FACET") -> TString() ),
//       facets.zipWithIndex
//             .map { case (facet, id) => 
//               Seq(IntPrimitive(id), StringPrimitive(facet.toJson.toString))
//             }
//     )
//     db.views.create(facetTable, facetTableData)
//     WarningModel(modelName, Seq(TInt(), TString()))
//   }
  
//   def tableCatalogFor(db: Database, config: MultilensConfig): Operator = 
//   {
//     val modelName:ID = config.args match {
//       case Seq() =>  ID("MIMIR_SHAPE_", config.schema)
//       case Seq(Var(modelN)) => modelN
//       case Seq(StringPrimitive(modelN)) => ID(modelN)
//     }
//     val facetTable = modelName
//     var base:Operator = HardTable(
//       Seq( ID("TABLE_NAME") -> TString()),
//       Seq(Seq(StringPrimitive(config.schema.id)))
//     )
//     db.query(db.catalog.tableOperator(facetTable)) { result =>
//       for(row <- result){ 
//         val facet = DatasetShape.parse(row(ID("FACET")).asString)

//         // First check if the facet is applicable.
//         val warnings = facet.test(db, config.query)

//         // It's not elegant, but use a selection predicate to 
//         // annotate the row.  The predicate should get compiled away
//         // but the data warning will be applied to the entire row.
//         for(warning <- warnings){
//           base = base.filter { 
//             DataWarning(
//               modelName, 
//               BoolPrimitive(true), 
//               StringPrimitive(warning), 
//               Seq(row(ID("ID")), StringPrimitive(warning))
//             )
//           }
//         }
//       }
//     }
//     return base
//   }
//   def attrCatalogFor(db: Database, config: MultilensConfig): Operator = 
//   {
//     HardTable(
//       Seq( 
//         ID("TABLE_NAME") -> TString(), 
//         ID("ATTR_NAME")  -> TString(),
//         ID("IS_KEY")     -> TBool(),
//         ID("ATTR_TYPE")  -> TType()
//       ),
//       db.typechecker.schemaOf(config.query).map { case (col, t) =>
//         Seq(
//           StringPrimitive(config.schema.id),  //Table Name
//           StringPrimitive(col.id),            //Column Name
//           BoolPrimitive(false),               //Is Key?
//           TypePrimitive(t)                    //Column Type
//         )
//       }
//     )
//   }

//   def viewFor(db: Database, config: MultilensConfig, table: ID): Option[Operator] = 
//     if(table.equals(config.schema)){
//       Some(config.query)
//     } else {
//       None
//     }
// }

