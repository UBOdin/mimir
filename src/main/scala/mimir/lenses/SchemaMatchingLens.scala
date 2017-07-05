package mimir.lenses

import java.sql.SQLException
import scala.util._

import mimir.Database
import mimir.algebra._
import mimir.models._
import org.apache.lucene.search.spell.{JaroWinklerDistance, LevensteinDistance, NGramDistance, StringDistance}

object SchemaMatchingLens {

  def create(
    db: Database, 
    name: String, 
    query: Operator, 
    args:Seq[Expression]
  ): (Operator, Seq[Model]) =
  {
    var targetSchema =
      args.
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
              s"$name:$modelCategory", 
              Left(query), 
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
        throw new SQLException("No valid schema matching model for column '"+target+"' in lens '"+name+"'");
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
                s"$name:META:$column", 
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

    val sourceColumns = query.columnNames

    val projectArgs = 
      schemaChoice.
        map({ case (targetCol, sourceChoiceModel) => {
          val sourceChoices = 
            sourceColumns.map( attr => 
              ( StringPrimitive(attr), Var(attr) )
            )
          ProjectArg(targetCol,
            ExpressionUtils.makeCaseExpression(
              sourceChoiceModel,
              sourceChoices,
              NullPrimitive()
            )
          )
        }})

    return ( 
      Project(projectArgs, query), 
      modelEntities ++ metaModels
    ) 
  }
}
