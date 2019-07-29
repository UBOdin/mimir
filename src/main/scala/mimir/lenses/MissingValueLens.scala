package mimir.lenses

import java.io.File
import java.sql._
import java.util

import mimir.algebra._
import mimir.ctables._
import mimir.util.RandUtils
import mimir.Database
import mimir.models._
import mimir.parser.ExpressionParser

import scala.collection.JavaConversions._
import scala.util._
import com.typesafe.scalalogging.slf4j.LazyLogging


object MissingValueLens extends LazyLogging {

  def getConstraint(arg: Expression): Seq[(ID, Expression)] =
  {
    arg match {
      case Var(v) => Seq( (v, Var(v).isNull.not) )
      case StringPrimitive(exprString) => {
        getConstraint(ExpressionParser.expr(exprString.replaceAll("''", "'")))
      }
      case e if Typechecker.trivialTypechecker.typeOf(e, (_:ID) => TAny() ).equals(TBool()) => {
        ExpressionUtils.getColumns(arg).toSeq match {
          case Seq(v) => 
            Seq( 
              (
                v, 
                Var(v).isNull.not.and(
                  Eval.inline(arg) { x => Var(x) }
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
    name: ID, 
    humanReadableName: String,
    query: Operator, 
    args:Seq[Expression]
  ): (Operator, Seq[Model]) =
  {
    logger.trace(s"Human readable name: ${humanReadableName}")

    // Preprocess the lens arguments...
    // Semantics are as follows:
    // 
    // - 'columnName' means that we should replace missing values of columnName
    // - columnName   means that we should replace missing values of columnName
    // - REQUIRE(column > ...) or a similar constraint means that we should replace 
    //   missing values and values that don't satisfy the constraint
    // - REQUIRE(column, column > ...) is similar, but allows you to define constraints
    //   over multiple columns

    val targetColumnsAndTests:Map[ID, Expression] = args.flatMap { getConstraint(_) }.toMap

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
      query.mapByID(
        schema.
          map { col => 
            targetColumnsAndTests.get(col) match {
              case Some(IsNullExpression(v)) if col.equals(v) => (col, Var(col))
              case Some(test)                                 => (col, test.thenElse { Var(col) } { NullPrimitive() })
              case None                                       => (col, Var(col))
            }
          }:_*
      )

    val modelsByType: Seq[(ID, Seq[(ID, (Model, Int, Seq[Expression]))])] =
      ModelRegistry.imputations.toSeq.map {
        case ( 
          modelCategory: ID, 
          constructor: ModelRegistry.ImputationConstructor
        ) => {
          val modelsByTypeAndColumn: Seq[(ID, (Model, Int, Seq[Expression]))] = 
            constructor(
              db, 
              ID(name, ":", modelCategory), 
              targetColumnsAndTests.keySet.toSeq, 
              noErroneousValuesQuery,
              humanReadableName
            ).toSeq
          
          (modelCategory, modelsByTypeAndColumn)
        }
      }

    val (
      candidateModels: Map[ID,Seq[(ID,Int,Seq[Expression],ID)]],
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
      replacementExprsList: Seq[(ID,Expression)], 
      metaModels: Seq[Model]
    ) =
      candidateModels.
        map({ 
          case (column, models) => {
            //TODO: Replace Default Model
            val metaModel = new DefaultMetaModel(
                ID(name, ":META:", column), 
                s"picking values for ${humanReadableName}.$column",
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

