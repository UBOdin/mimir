package mimir.lenses

import java.io.File
import java.sql._
import java.util

import mimir.algebra._
import mimir.ctables._
import mimir.util.RandUtils
import mimir.{Analysis, Database}
import mimir.models._

import scala.collection.JavaConversions._
import scala.util._

object MissingValueLens {

  def create(
    db: Database, 
    name: String, 
    query: Operator, 
    args:Seq[Expression]
  ): (Operator, Seq[Model]) =
  {
    val targetColumns:List[String] = args.map(db.interpreter.evalString(_).toUpperCase).toSet.toList
    val schema:Set[String] = query.columnNames.toSet
    val missingColumns = targetColumns.toSet -- schema

    if(!missingColumns.isEmpty){
      throw new SQLException(
        "Invalid missing value lens: ["+missingColumns.mkString(", ")+
        "] not part of ["+schema.mkString(", ")+"]"
      )
    }

    val modelsByType: Seq[(String, Seq[(String, (Model, Int, Seq[Expression]))])] =
      ModelRegistry.imputations.toSeq.map {
        case ( 
          modelCategory: String, 
          constructor: ModelRegistry.ImputationConstructor
        ) => {
          val modelsByTypeAndColumn: Seq[(String, (Model, Int, Seq[Expression]))] = 
            constructor(
              db, 
              s"$name:$modelCategory", 
              targetColumns, 
              query
            ).toSeq
          
          (modelCategory, modelsByTypeAndColumn)
        }
      }

    val (
      candidateModels: Map[String,Seq[(String,Int,Seq[Expression],String)]],
      modelEntities: Seq[Model]
    ) = 
      LensUtils.extractModelsByColumn(modelsByType)

    // Sanity check...
    targetColumns.foreach( target => {
      if(!candidateModels.contains(target)){ 
        throw new SQLException("No valid imputation model for column '"+target+"' in lens '"+name+"'");
      }
    })

    val (
      replacementExprsList: Seq[(String,Expression)], 
      metaModels: Seq[Model]
    ) =
      candidateModels.
        map({ 
          case (column, models) => {
            //TODO: Replace Default Model
            val metaModel = new DefaultMetaModel(
                s"$name:META:$column", 
                s"picking a value for column '$column'",
                models.map(_._4)
              )
            val metaExpr = LensUtils.buildMetaModel(
              metaModel.name, 0, Seq(), Seq(),
              models, Seq[Expression](RowIdVar())
            )

            ( (column, metaExpr), metaModel )
          }
        }).
        unzip

    val replacementExprs = replacementExprsList.toMap
    val projectArgs = 
      query.columnNames.
        map( col => replacementExprs.get(col) match {
          case None => ProjectArg(col, Var(col))
          case Some(replacementExpr) => 
            ProjectArg(col,
              Conditional(
                IsNullExpression(Var(col)),
                replacementExpr,
                Var(col)
              ))
        })

    return (
      Project(projectArgs, query),
      modelEntities ++ metaModels
    )
  }

}

