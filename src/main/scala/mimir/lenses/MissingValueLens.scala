package mimir.lenses

import java.io.File
import java.sql._
import java.util

import mimir.algebra._
import mimir.ctables._
import mimir.util.RandUtils
import mimir.{Analysis, Database}
import mimir.models._
import mimir.parser.ExpressionParser

import scala.collection.JavaConversions._
import scala.util._


object MissingValueLens {

  def getConstraint(arg: Expression): Seq[(String, Expression)] =
  {
    arg match {
      case Var(v) => Seq( (v.toUpperCase, Var(v.toUpperCase).isNull.not) )
      case StringPrimitive(exprString) => {
        getConstraint(ExpressionParser.expr(exprString))
      }
      case e if Typechecker.trivialTypechecker.typeOf(e, (_:String) => TAny() ).equals(TBool()) => {
        ExpressionUtils.getColumns(arg).toSeq match {
          case Seq(v) => 
            Seq( 
              (
                v.toUpperCase, 
                Var(v.toUpperCase).isNull.not.and(
                  Eval.inline(arg) { x => Var(x.toUpperCase) }
                )
              )
            )
          case Seq() => throw new RAException(s"Invalid Constraint $e (need a variable in require)")
          case _ => throw new RAException(s"Invalid Constraint $e (one variable per require)")
        }
      }
      case e =>
        throw new RAException("Invalid constraint $e (Not a Boolean Expression)")
    }
  }


  def create(
    db: Database, 
    name: String, 
    query: Operator, 
    args:Seq[Expression]
  ): (Operator, Seq[Model]) =
  {

    // Preprocess the lens arguments...
    // Semantics are as follows:
    // 
    // - 'columnName' means that we should replace missing values of columnName
    // - columnName   means that we should replace missing values of columnName
    // - REQUIRE(column > ...) or a similar constraint means that we should replace 
    //   missing values and values that don't satisfy the constraint
    // - REQUIRE(column, column > ...) is similar, but allows you to define constraints
    //   over multiple columns

    val targetColumnsAndTests:Map[String, Expression] = args.flatMap { getConstraint(_) }.toMap

    // Sanity check.  Require that all columns that we fix and all columns referenced in the 
    // constraints are defined in the original query.
    val schema = query.columnNames
    val requiredColumns = targetColumnsAndTests.values.flatMap { ExpressionUtils.getColumns(_) }
    val missingColumns = (targetColumnsAndTests.keySet ++ requiredColumns) -- schema.toSet
    if(!missingColumns.isEmpty){
      throw new SQLException(
        "Invalid missing value lens: ["+missingColumns.mkString(", ")+
        "] not part of ["+schema.mkString(", ")+"]"
      )
    }

    // Create a query where all values that dont satisfy their constraints are removed
    // (used for training the models)
    val noErroneousValuesQuery =
      query.map(
        schema.
          map { col => 
            targetColumnsAndTests.get(col) match {
              case Some(IsNullExpression(v)) if col.equals(v) => (col, Var(col))
              case Some(test)                                 => (col, test.thenElse { Var(col) } { NullPrimitive() })
              case None                                       => (col, Var(col))
            }
          }:_*
      )

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
              targetColumnsAndTests.keySet.toSeq, 
              noErroneousValuesQuery
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
    targetColumnsAndTests.keySet.foreach( target => {
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
                s"picking values for $name.$column",
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
                targetColumnsAndTests(col),
                Var(col),
                replacementExpr
              ))
        })

    return (
      Project(projectArgs, query),
      modelEntities ++ metaModels
    )
  }

}

