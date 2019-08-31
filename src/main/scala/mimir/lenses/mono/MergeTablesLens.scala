package mimir.lens.mono

import play.api.libs.json._

import mimir.algebra._
import mimir.Database
import mimir.lenses._

object MergeTablesLens extends MonoLens
{
  def train(
    db: Database,
    name: ID,
    query: Operator,
    config: JsValue
  ): JsValue = ???

  def view(
    db: Database,
    name: ID,
    query: Operator,
    config: JsValue,
    friendlyName: String
  ): Operator = ???
}


// package mimir.adaptive

// import com.typesafe.scalalogging.slf4j.LazyLogging
// import mimir.Database
// import mimir.algebra._
// import mimir.lenses._
// import mimir.models._
// import mimir.util.SqlUtils
// import scala.util.Random

// object SchemaMatching
//   extends Multilens
//     with LazyLogging
// {


//   def initSchema(db: Database, config: MultilensConfig): TraversableOnce[Model] =
//   {
//     logger.debug(s"Creating SchemaMatching: $config")
//     val viewName = config.schema
    
//     val targetSchema =
//       config.args.
//         map(field => {
//           val split = db.interpreter.evalString(field).split(" +")
//           val varName  = ID(split(0))
//           val typeName = split(1)
//           (
//             varName -> Type.fromString(typeName.toString)
//           )
//         }).
//         toList

//     val modelsByType = 
//       ModelRegistry.schemamatchers.toSeq.map {
//         case (
//           modelCategory:ID, 
//           constructor:ModelRegistry.SchemaMatchConstructor
//         ) => {
//           val modelsByColAndType =
//             constructor(
//               db, 
//               ID(viewName, ":", modelCategory),
//               Left(config.query), 
//               Right(targetSchema)
//             ).toSeq.map {
//               case (col, (model, idx)) => (col, (model, idx, Seq[Expression]()))
//             }

//           (modelCategory, modelsByColAndType)
//         }
//       }

//     val (
//       candidateModels: Map[ID,Seq[(ID,Int,Seq[Expression],ID)]],
//       modelEntities: Seq[Model]
//     ) = 
//       LensUtils.extractModelsByColumn(modelsByType)

//     // Sanity check...
//     targetSchema.map(_._1).foreach( target => {
//       if(!candidateModels.contains(target)){ 
//         throw new Exception("No valid schema matching model for column '"+target+"' in lens '"+viewName+"'");
//       }
//     })
    
//     val (
//       schemaChoice:List[(ID,Expression)], 
//       metaModels:List[Model]
//     ) =
//       candidateModels.
//         toList.
//         map({ 
//           case (column, models) => {
//             //TODO: Replace Default Model
//             val metaModel = new DefaultMetaModel(
//                 ID(viewName,":META:", column), 
//                 s"picking a source for column '$column'",
//                 models.map(_._4)
//               )
//             val metaExpr = LensUtils.buildMetaModel(
//               metaModel.name, 0, Seq[Expression](), Seq[Expression](),
//               models, Seq[Expression]()
//             )

//             ( (column, metaExpr), metaModel )
//           }
//         }).
//         unzip

        
//     modelEntities ++ metaModels
//   }

//   def tableCatalogFor(db: Database, config: MultilensConfig): Operator =
//   {
//     HardTable(
//       Seq(
//         ID("TABLE_NAME") -> TString()
//       ),
//       Seq(
//         Seq(
//           StringPrimitive("DATA")
//         )
//       )
//     )
//   }
  
//   def attrCatalogFor(db: Database, config: MultilensConfig): Operator =
//   {
//     val sourceSchema = db.typechecker.schemaOf(config.query)
//     val targetSchema =
//       config.args.
//         map(field => {
//           val split = db.interpreter.evalString(field).split(" +")
//           val varName  = split(0).toUpperCase
//           val typeName = split(1)
//           (
//             varName.toString.toUpperCase -> 
//               Type.fromString(typeName.toString)
//           )
//         }).toList
//     val schemaModel = db.models.get(config.schema.withSuffix(":META:"+targetSchema.head._1))
//     val schemaGuess = schemaModel.bestGuess(0, Seq(), Seq())
//     val nameMatchModelName = config.schema.withSuffix(s":${schemaGuess.asString}:${targetSchema.head._1}")
//     targetSchema.tail.foldLeft(
//       Select(Comparison(Cmp.Eq, 
//           VGTerm(nameMatchModelName, 0, Seq(), Seq()), 
//           Var(ID("SRC_ATTR_NAME"))
//         ),
//         Join(
//           HardTable( Seq(
//               ID("ATTR_NAME") -> TString(), 
//               ID("ATTR_TYPE") -> TType()
//             ), 
//             Seq( 
//               Seq( StringPrimitive(targetSchema.head._1), TypePrimitive(targetSchema.head._2))
//             )
//           ),
//           HardTable( Seq(
//               ID("SRC_ATTR_NAME") -> TString(), 
//               ID("SRC_ATTR_TYPE") -> TType()
//             ), 
//             sourceSchema.map(scol => 
//               Seq( StringPrimitive(scol._1.id), TypePrimitive(scol._2) )
//             )
//           )
//         )
//       ):Operator)((init, col) => {
//         val schemaModel = db.models.get(config.schema.withSuffix(s":META:${col._1}"))
//         val schemaModelGuess = schemaModel.bestGuess(0, Seq(), Seq())
//         val cmpModelName = config.schema.withSuffix(s":${schemaModelGuess.asString}:${col._1}")
//         Union(init, 
//           Select(Comparison(Cmp.Eq, 
//               VGTerm(cmpModelName, 0, Seq(), Seq()), 
//               Var(ID("SRC_ATTR_NAME"))
//             ),
//             Join(
//               HardTable( Seq(
//                   ID("ATTR_NAME") ->  TString(), 
//                   ID("ATTR_TYPE") ->  TType()
//                 ), 
//                 Seq(Seq( StringPrimitive(col._1), TypePrimitive(col._2)))
//               ),
//               HardTable( Seq(
//                   ID("SRC_ATTR_NAME") -> TString(), 
//                   ID("SRC_ATTR_TYPE") -> TType()
//                 ), 
//                 sourceSchema.map(scol => 
//                   Seq( StringPrimitive(scol._1.id), TypePrimitive(scol._2))
//                 )
//               )
//             )
//         ))
//     }).addColumns("TABLE_NAME" -> StringPrimitive("DATA"))
//       .addColumns("IS_KEY" -> BoolPrimitive(false))
//       .removeColumns("SRC_ATTR_NAME", "SRC_ATTR_TYPE")
//   }
        
//   def viewFor(db: Database, config: MultilensConfig, table: ID): Option[Operator] =
//   {
//     if(table.equalsIgnoreCase("DATA")){
//       val targetSchema =
//       config.args.
//         map(field => {
//           val split = db.interpreter.evalString(field).split(" +")
//           val varName  = ID(split(0))
//           val typeName = ID(split(1))
//           (
//             varName -> 
//               Type.fromString(typeName.toString)
//           )
//         }).toList
//       Some(Project(
//         targetSchema.map { case (colName, colType)  => { 
//           val metaModel = db.models.get(ID(config.schema,s":META:$colName"))
//           val model = db.models.get(config.schema+ID(":")+ID(metaModel
//             .bestGuess(0, Seq(), Seq()).asString+":")+colName)
//           ProjectArg(colName,  model.bestGuess(0, Seq(), Seq()) match {
//             case np@NullPrimitive() => np
//             case x => Var(ID(x.asString))
//           } )  
//         }}, config.query
//       ))  
//     } else { None }
//   } 
// }


