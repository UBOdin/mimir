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

object PickerLens {

  def create(
    db: Database, 
    name: String, 
    query: Operator, 
    args:Seq[Expression]
  ): (Operator, Seq[Model]) =
  {
    val operSchema = query.schema
    val schemaMap = operSchema.toMap
    val (pickFromColumns, pickerColTypes ) = args.flatMap {
      case Var(col) => Some((col, schemaMap(col)))
      case _ => None
    }.unzip
    
    val resultColName = "PICK_ONE_" +pickFromColumns.mkString("_") 
    
    val pickerModel = new PickerModel(name+"_PICKER_MODEL", resultColName, pickFromColumns, pickerColTypes, query) 
    pickerModel.reconnectToDatabase(db)
    
    val pickUncertainExprs : List[(Expression, Expression)] = args.flatMap {
      case Function("UEXPRS", Seq(StringPrimitive(expr), StringPrimitive(resultExpr)) ) => Some( (
          db.parseExpression(expr), 
          VGTerm(pickerModel, 0,Seq[Expression](RowIdVar()), Seq(db.parseExpression(resultExpr))) 
          ) )
      case _ => None
    }.toList
    
    val pickCertainExprs : List[(Expression, Expression)] = args.flatMap {
      case Function("EXPRS", Seq(StringPrimitive(expr), StringPrimitive(resultExpr)) ) => Some( (
          db.parseExpression(expr), 
          db.parseExpression(resultExpr)
          ) )
      case _ => None
    }.toList
    
    val pickExpr = ExpressionUtils.makeCaseExpression(
          pickCertainExprs.union(pickUncertainExprs),
          NullPrimitive()
        )
    
    val pickerColsTypeMap = pickFromColumns.zip(pickerColTypes).toMap
    
    val projectArgs = 
      query.schema.
        map(_._1).
        flatMap( col => pickerColsTypeMap.get(col) match {
          case None => Some(ProjectArg(col, Var(col)))
          case Some(pickFromCol) => None
        }).union(Seq(ProjectArg(resultColName, pickExpr)))

    return (
      Project(projectArgs, query),
      Seq(pickerModel)
    )
  }
}

