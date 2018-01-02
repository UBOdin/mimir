package mimir.adaptive

import com.typesafe.scalalogging.slf4j.LazyLogging
import mimir.Database
import mimir.algebra._
import mimir.lenses._
import mimir.models._
import mimir.util.SqlUtils
import scala.util.Random

object SchemaMatching
  extends Multilens
    with LazyLogging
{


  def initSchema(db: Database, config: MultilensConfig): TraversableOnce[Model] =
  {
    logger.debug(s"Creating SchemaMatching: $config")
    val viewName = config.schema
    
    val targetSchema =
      config.args.
        map(field => {
          val split = db.interpreter.evalString(field).split(" +")
          val varName  = split(0).toUpperCase
          val typeName = split(1)
          (
            varName.toString.toUpperCase -> 
              Type.fromString(typeName.toString)
          )
        }).
        toList

    val modelsByType = 
      ModelRegistry.schemamatchers.toSeq.map {
        case (
          modelCategory:String, 
          constructor:ModelRegistry.SchemaMatchConstructor
        ) => {
          val modelsByColAndType =
            constructor(
              db, 
              s"$viewName:$modelCategory", 
              Left(config.query), 
              Right(targetSchema)
            ).toSeq.map {
              case (col, (model, idx)) => (col, (model, idx, Seq[Expression]()))
            }

          (modelCategory, modelsByColAndType)
        }
      }

    val (
      candidateModels: Map[String,Seq[(String,Int,Seq[Expression],String)]],
      modelEntities: Seq[Model]
    ) = 
      LensUtils.extractModelsByColumn(modelsByType)

    // Sanity check...
    targetSchema.map(_._1).foreach( target => {
      if(!candidateModels.contains(target)){ 
        throw new Exception("No valid schema matching model for column '"+target+"' in lens '"+viewName+"'");
      }
    })
    
    val (
      schemaChoice:List[(String,Expression)], 
      metaModels:List[Model]
    ) =
      candidateModels.
        toList.
        map({ 
          case (column, models) => {
            //TODO: Replace Default Model
            val metaModel = new DefaultMetaModel(
                s"$viewName:META:$column", 
                s"picking a source for column '$column'",
                models.map(_._4)
              )
            val metaExpr = LensUtils.buildMetaModel(
              metaModel.name, 0, Seq[Expression](), Seq[Expression](),
              models, Seq[Expression]()
            )

            ( (column, metaExpr), metaModel )
          }
        }).
        unzip

        
    modelEntities ++ metaModels
  }

  def tableCatalogFor(db: Database, config: MultilensConfig): Operator =
  {
    HardTable(
      Seq(
        ("TABLE_NAME",TString())
      ),
      Seq(
        Seq(
          StringPrimitive("DATA")
        )
      )
    )
  }
  
  def attrCatalogFor(db: Database, config: MultilensConfig): Operator =
  {
    val sourceSchema = db.typechecker.schemaOf(config.query)
    val targetSchema =
      config.args.
        map(field => {
          val split = db.interpreter.evalString(field).split(" +")
          val varName  = split(0).toUpperCase
          val typeName = split(1)
          (
            varName.toString.toUpperCase -> 
              Type.fromString(typeName.toString)
          )
        }).toList
    targetSchema.tail.foldLeft(
      Select(Comparison(Cmp.Eq, VGTerm(s"${config.schema}:${db.models.get(s"${config.schema}:META:${targetSchema.head._1}")
          .bestGuess(0, Seq(), Seq()).asString}:${targetSchema.head._1}", 0, Seq(), Seq()), Var("SRC_ATTR_NAME")),
        Join(
          HardTable( Seq(("ATTR_NAME" , TString()), ("ATTR_TYPE", TType())), 
              sourceSchema.map(col => Seq( StringPrimitive(col._1), TypePrimitive(col._2)))),
          HardTable( Seq(("SRC_ATTR_NAME" , TString()), ("SRC_ATTR_TYPE", TType())), 
              targetSchema.map(col => Seq( StringPrimitive(col._1), TypePrimitive(col._2))))
        )
      ):Operator)((init, col) => {
        Union(init, 
          Select(Comparison(Cmp.Eq, VGTerm(s"${config.schema}:${db.models.get(s"${config.schema}:META:${col._1}")
            .bestGuess(0, Seq(), Seq()).asString}:${col._1}", 0, Seq(), Seq()), Var("SRC_ATTR_NAME")),
            Join(
              HardTable( Seq(("ATTR_NAME" , TString()), ("ATTR_TYPE", TType())), 
                  sourceSchema.map(col => Seq( StringPrimitive(col._1), TypePrimitive(col._2)))),
              HardTable( Seq(("SRC_ATTR_NAME" , TString()), ("SRC_ATTR_TYPE", TType())), 
                  targetSchema.map(col => Seq( StringPrimitive(col._1), TypePrimitive(col._2))))
            )
        ))
    }).addColumn("TABLE_NAME" -> StringPrimitive("DATA")).addColumn("IS_KEY" -> BoolPrimitive(false))
    .removeColumns("SRC_ATTR_NAME", "SRC_ATTR_TYPE")
  }
        
  def viewFor(db: Database, config: MultilensConfig, table: String): Option[Operator] =
  {
    if(table.equals("DATA")){
      val targetSchema =
      config.args.
        map(field => {
          val split = db.interpreter.evalString(field).split(" +")
          val varName  = split(0).toUpperCase
          val typeName = split(1)
          (
            varName.toString.toUpperCase -> 
              Type.fromString(typeName.toString)
          )
        }).toList
      Some(Project(
        targetSchema.map { case (colName, colType)  => { 
          val metaModel = db.models.get(s"${config.schema}:META:$colName")
          val model = db.models.get(s"${config.schema}:${metaModel
            .bestGuess(0, Seq(), Seq()).asString}:$colName")
          ProjectArg(colName,  Var(model.bestGuess(0, Seq(), Seq()).asString) )
        }}, config.query
      ))  
    } else { None }
  } 
}


