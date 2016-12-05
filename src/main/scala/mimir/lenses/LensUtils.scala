package mimir.lenses

import java.sql.SQLException

import mimir.algebra._
import mimir.models._
import mimir.ctables._

object LensUtils {

  def extractModelsByColumn[CONSTR_TYPE](
    constructors:List[(String,CONSTR_TYPE)],
    callConstructor:((String,CONSTR_TYPE) => Map[String,(Model,Int)])
  ): (Map[String,List[(Model,Int,String)]], List[Model]) =
  {
    val candidateModels =
      constructors.
        map({ case (modelCategory, constructor) => {
          val modelCandidates = 
            callConstructor(modelCategory,constructor)

          modelCandidates.toList.
            map({ case (column, (model, idx)) =>
              (column, (model, idx, modelCategory))
            })
        }}).
        flatten.
        groupBy(_._1).            // Group candidates by target column
        map( x => (x._1, x._2.map(_._2)) )   // Convert to Column -> List[(Model, Idx, Category)]

    val modelEntities =
      candidateModels.         // Start with the models per column
        values.                // We only care about the models themselves
        flatten.               // Unnest the lists
        groupBy( _._1.name ).  // Organize by model name
        map( _._2.head ).      // We only need one copy of each model
        map( _._1).            // We only care about the model itself
        toList

    (candidateModels, modelEntities)
  }
 
  def buildMetaModel(
    metaModel: Model, 
    metaModelIdx: Int,
    metaModelArgs: List[Expression],
    inputModels:List[(Model,Int,String)], 
    inputArgs: List[Expression]
  ): Expression =
  {
    val inputVGTerms =
      inputModels.
        map({case (model, idx, cat) => (
          StringPrimitive(cat), 
          VGTerm(model, idx, inputArgs)
        )})

    inputVGTerms match {
      case Nil => 
        throw new SQLException("No valid models to be wrapped")
      case List((_, term)) => term
      case _ => 
        ExpressionUtils.makeCaseExpression(
          VGTerm(metaModel, metaModelIdx, metaModelArgs),
          inputVGTerms,
          NullPrimitive()
        )
    }
  }
}