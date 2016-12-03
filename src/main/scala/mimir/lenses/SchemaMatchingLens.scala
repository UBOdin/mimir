package mimir.lenses

import java.sql.SQLException
import scala.util._

import mimir.Database
import mimir.algebra._
import mimir.ctables.VGTerm
import mimir.optimizer.{InlineVGTerms}
import mimir.models._
import org.apache.lucene.search.spell.{JaroWinklerDistance, LevensteinDistance, NGramDistance, StringDistance}

object SchemaMatchingLens {

  def create(
    db: Database, 
    name: String, 
    query: Operator, 
    args:List[Expression]
  ): (Operator, List[Model]) =
  {
    var targetSchema =
      args.
        map(field => {
          val split = Eval.evalString(field).split(" +")
          val varName  = split(0).toUpperCase
          val typeName = split(1)
          (
            varName.toString.toUpperCase -> 
              Type.fromString(typeName.toString)
          )
        }).
        toList

    val (
      candidateModels: Map[String,List[(Model,Int,String)]],
      modelEntities: List[Model]
    ) = 
      LensUtils.extractModelsByColumn(
        ModelRegistry.schemamatchers.toList,
        (
          modelCategory:String, 
          constructor:ModelRegistry.SchemaMatchConstructor
        ) => 
            constructor(
              db, 
              s"$name:$modelCategory", 
              Left(query), 
              Right(targetSchema)
            )
      )

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
                models.map(_._3)
              )
            val metaExpr = LensUtils.buildMetaModel(
              metaModel, 0, List[Expression](), 
              models, List[Expression]()
            )

            ( (column, metaExpr), metaModel )
          }
        }).
        unzip

    val sourceColumns = query.schema.map(_._1)

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
