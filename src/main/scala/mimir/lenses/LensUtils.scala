package mimir.lenses

import java.sql.SQLException

import mimir.algebra._
import mimir.models._
import mimir.ctables._
import mimir.util.NameLookup
import sparsity.Name

object LensUtils {

  def columnLookupFunction(query: Operator) =
  {
    val columnLookup = NameLookup.fromID(query.columnNames)
    val queryColumns = query.columnNames

    (col: String) => {
      columnLookup(Name(col)).getOrElse {
        throw new SQLException(s"Invalid target column: $col; Available columns are ${queryColumns.mkString(", ")}")


      }
    }
  }

  def extractModelsByColumn(
    modelMap: Seq[
      ( ID,                   // Model Group
        Seq[                  
          ( ID,               // Column
            ( Model,          // Model Name
              Int,            // Model Index
              Seq[Expression] // Hint Expressions
            )
          )
        ])
    ]
  ): (Map[ID,Seq[(ID,Int,Seq[Expression],ID)]], Seq[Model]) =
  {
    val modelsByColumn: Seq[(ID, (ID, Int, Seq[Expression], ID))] =
      modelMap.
        flatMap {
          case (modelGroup, perColumnImplementations) =>
            perColumnImplementations.map {
              case (col:ID, (model, idx, hints)) => 
                (col, (model.name, idx, hints, modelGroup))
            }
        }
    val candidateModels:Map[ID, Seq[(ID, Int, Seq[Expression], ID)]] =
      modelsByColumn
        .groupBy { _._1 }            // Group candidates by target column
        .mapValues { _.map(_._2) }   // Convert to Column -> List[(Model, Idx, Category)]

    val allModels: Seq[Model] =
      modelMap                 // Start with the models
        .flatMap { _._2.map { _._2._1 } }

    val modelEntities: Seq[Model] =
      allModels                // We only care about the models themselves
        .groupBy( _.name )     // Organize by model name
        .toSeq
        .map { (_:(ID, Seq[Model]))._2.head }     // We only need one copy of each model

    (candidateModels, modelEntities)
  }
 
  def buildMetaModel(
    metaModel: ID, 
    metaModelIdx: Int,
    metaModelArgs: Seq[Expression],
    metaModelHints: Seq[Expression],
    inputModels: Seq[(ID,Int,Seq[Expression],ID)], 
    inputArgs: Seq[Expression]
  ): Expression =
  {
    val inputVGTerms =
      inputModels.
        map({case (model, idx, hints, cat) => (
          StringPrimitive(cat.id), 
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