package mimir.lenses

import java.sql.SQLException

import mimir.algebra._
import mimir.models._
import mimir.ctables._

object LensUtils {

  def extractModelsByColumn(
    modelMap: Seq[(String, Seq[(String, (Model, Int, Seq[Expression]))])]
  ): (Map[String,Seq[(Model,Int,Seq[Expression],String)]], Seq[Model]) =
  {
    val candidateModels =
      modelMap.
        flatMap {
          case (modelGroup, perColumnImplementations) =>
            perColumnImplementations.map {
              case (col, (model, idx, hints)) => 
                (col, (model, idx, hints, modelGroup))
            }
        }.
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
    metaModelArgs: Seq[Expression],
    metaModelHints: Seq[Expression],
    inputModels: Seq[(Model,Int,Seq[Expression],String)], 
    inputArgs: Seq[Expression]
  ): Expression =
  {
    val inputVGTerms =
      inputModels.
        map({case (model, idx, hints, cat) => (
          StringPrimitive(cat), 
          VGTerm(model, idx, inputArgs, hints)
        )})

    inputVGTerms match {
      case Nil => 
        throw new SQLException("No valid models to be wrapped")
      case List((_, term)) => term
      case _ => 
        ExpressionUtils.makeCaseExpression(
          VGTerm(metaModel, metaModelIdx, metaModelArgs, metaModelHints),
          inputVGTerms,
          NullPrimitive()
        )
    }
  }
}