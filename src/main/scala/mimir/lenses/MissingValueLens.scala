package mimir.lenses

import java.io.File
import java.sql._
import java.util

import mimir.algebra._
import mimir.ctables._
import mimir.exec.ResultIterator
import mimir.util.RandUtils
import mimir.{Analysis, Database}
import moa.classifiers.Classifier
import moa.core.InstancesHeader
import weka.core.{Attribute, DenseInstance, Instance, Instances}
import weka.experiment.{DatabaseUtils, InstanceQueryAdapter}
import mimir.optimizer.InlineVGTerms
import mimir.models._

import scala.collection.JavaConversions._
import scala.util._

object MissingValueLens {

  def create(
    db: Database, 
    name: String, 
    query: Operator, 
    args:List[Expression]
  ): (Operator, List[Model]) =
  {
    val targetColumns = args.map(Eval.evalString(_).toUpperCase).toSet.toList

    val (
      candidateModels: Map[String,List[(Model,Int,String)]],
      modelEntities: List[Model]
    ) = 
      LensUtils.extractModelsByColumn(
        ModelRegistry.imputations.toList,
        ( modelCategory:String, 
          constructor:ModelRegistry.ImputationConstructor
        ) => 
            constructor(
              db, 
              s"$name:$modelCategory", 
              targetColumns, 
              query
            )
      )

    // Sanity check...
    targetColumns.foreach( target => {
      if(!candidateModels.contains(target)){ 
        throw new SQLException("No valid imputation model for column '"+target+"' in lens '"+name+"'");
      }
    })

    val (
      replacementExprsList:List[(String,Expression)], 
      metaModels:List[Model]
    ) =
      candidateModels.
        toList.
        map({ 
          case (column, models) => {
            //TODO: Replace Default Model
            val metaModel = new DefaultMetaModel(
                s"$name:META:$column", 
                s"picking a value for column '$column'",
                models.map(_._3)
              )
            val metaExpr = LensUtils.buildMetaModel(
              metaModel, 0, List[Expression](), 
              models, List[Expression](RowIdVar())
            )

            ( (column, metaExpr), metaModel )
          }
        }).
        unzip

    val replacementExprs = replacementExprsList.toMap
    val projectArgs = 
      query.schema.
        map(_._1).
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

