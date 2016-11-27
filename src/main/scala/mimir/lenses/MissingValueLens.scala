package mimir.lenses

import java.io.File
import java.sql._
import java.util

import mimir.algebra._
import mimir.ctables._
import mimir.exec.ResultIterator
import mimir.util.{TypeUtils,RandUtils}
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
    val candidateModels: Map[String,List[(Model,Int,String)]] = 
      ModelRegistry.
        imputations.
        toList.
        map({ case (modelCategory, constructor) => {
          val modelCandidates = 
            constructor(
              db, 
              s"$name:$modelCategory", 
              targetColumns, 
              query
            ).toList

          modelCandidates.
            map({ case (column, (model, idx)) =>
              (column, (model, idx, modelCategory))
            })
        }}).
        flatten.
        groupBy(_._1).            // Group candidates by target column
        map( x => (x._1, x._2.map(_._2)) )   // Convert to Column -> List[(Model, Idx, Category)]

    var modelEntities:List[Model] = 
      candidateModels.         // Start with the models per column
        values.                // We only care about the models themselves
        flatten.               // Unnest the lists
        groupBy( _._1.name ).  // Organize by model name
        map( _._2.head ).      // We only need one copy of each model
        map( _._1).            // We only care about the model itself
        toList

    // Sanity check...
    targetColumns.foreach( target => {
      if(!candidateModels.contains(target)){ 
        throw new SQLException("No valid imputation model for column '"+target+"' in lens '"+name+"'");
      }
    })

    val (
      replacementExprsList:List[(String,Expression)], 
      metaModels:List[Option[Model]]
    ) =
      candidateModels.
        toList.
        map({ 
          case (column, Nil) => 
            throw new SQLException("No valid imputation model for column '"+column+"' in lens '"+name+"'");
          case (column, singleton::Nil) => 
            ( (column, makeVGTerm(singleton)), None )
          case (column, models) => {
            val metaModel = new MissingValueMetaModel(s"$name:META:$column", column)
            metaModel.train(db, query, column, models)
            val vgtermsForEachCategory = 
              models.map( x => 
                (StringPrimitive(x._3), makeVGTerm(x))
              )
            ( (column, 
                ExpressionUtils.makeCaseExpression(
                  makeVGTerm((metaModel,0,"")),
                  vgtermsForEachCategory,
                  NullPrimitive()
                )
              ), Some(metaModel) 
            )
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
      modelEntities ++ metaModels.flatten
    )
  }

  private def makeVGTerm(modelWithIndex:(Model,Int,String)): Expression =
  {
    VGTerm(modelWithIndex._1, modelWithIndex._2, List(RowIdVar()))
  }
}

class MissingValueMetaModel(name: String, colName: String) 
  extends SingleVarModel(name) with Serializable
{
  var votes: List[(String,Double)] = null

  def train(db:Database, query:Operator, column:String, models:List[(Model,Int,String)]) =
  {
    // TODO: Actually do some learning here...
    // For now, pick at random.  Whatever's first on the list.
    votes = (models.head._3, 1.0) :: (models.tail.map( x => (x._3, 0.0) ))
  }

  def varType(argTypes:List[Type.T]) = Type.TString

  def bestGuess(args: List[PrimitiveValue]): PrimitiveValue =
    StringPrimitive(votes.maxBy(_._2)._1)
  def sample(randomness: Random, args: List[PrimitiveValue]): PrimitiveValue =
    StringPrimitive(RandUtils.pickFromWeightedList(randomness, votes))
  def reason(args: List[Expression]): String =
  {
    val bestChoice = votes.maxBy(_._2)._1
    s"I decided to use '$bestChoice' to pick values for column '$colName'"
  }
}