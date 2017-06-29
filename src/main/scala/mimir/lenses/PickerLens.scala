package mimir.lenses

import java.io.File
import java.sql._
import java.util

import mimir.algebra._
import mimir.ctables._
import mimir.util.RandUtils
import mimir.Database
import mimir.parser._
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
    val operSchema = db.typechecker.schemaOf(query)
    val schemaMap = operSchema.toMap
    
    val (pickFromColumns, pickerColTypes ) = args.flatMap {
      case Function("PICK_FROM", cols ) => 
        Some( cols.map { case col:Var => (col.name, schemaMap(col.name)) 
                         case col => throw new RAException(s"Invalid pick_from argument: $col in PickerLens $name (not a column reference)")
                       } )
      case _ => None
    }.toSeq.flatten.unzip
    
    val pickToCol = args.flatMap {
      case Function("PICK_AS", Seq(Var(col))) => Some( col )
      case _ => None
    }
    
    val projectedOutPicFromCols = args.flatMap {
      case Function("HIDE_PICK_FROM", Seq(Var(col))) => Some( col )
      case _ => None
    }
    
    val resultColName = pickToCol.length match {
      case 0 => "PICK_ONE_" +pickFromColumns.mkString("_") 
      case 1 => pickToCol.head
    }
    
    val pickerModel = new PickerModel(name+"_PICKER_MODEL:"+pickFromColumns.mkString("_"), resultColName, pickFromColumns, pickerColTypes, query) 
    pickerModel.reconnectToDatabase(db)
    
    lazy val expressionSubstitutions : (Expression) => Expression = (expr) => {
    expr match {
      case Function("AVG", Seq(Var(col))) => {
        val exprSubQ = Project(Seq(ProjectArg(s"AVG_$col", expr)), query)
        db.query(exprSubQ)( results => {
        var replacementExpr : Expression = NullPrimitive()
        if(results.hasNext()){
          replacementExpr = results.next()(0)
        }
        results.close()
        replacementExpr
        })
      }
      case x => x.recur(expressionSubstitutions(_))
    }
  }
    
    val pickUncertainExprs : List[(Expression, Expression)] = args.flatMap {
      case Function("UEXPRS", Seq(StringPrimitive(expr), StringPrimitive(resultExpr)) ) => Some( (
          ExpressionParser.expr(expr), 
          VGTerm(pickerModel.name, 0,Seq[Expression](RowIdVar()), Seq(expressionSubstitutions(ExpressionParser.expr(resultExpr)))) 
          ) )
      case _ => None
    }.toList
    
    val pickCertainExprs : List[(Expression, Expression)] = args.flatMap {
      case Function("EXPRS", Seq(StringPrimitive(expr), StringPrimitive(resultExpr)) ) => Some( (
          ExpressionParser.expr(expr), 
          expressionSubstitutions(ExpressionParser.expr(resultExpr))
          ) )
      case _ => None
    }.toList
    
    val pickExpr = ExpressionUtils.makeCaseExpression(
          pickCertainExprs.union(pickUncertainExprs),
          NullPrimitive()
        )
    
    val pickerColsTypeMap = pickFromColumns.zip(pickerColTypes).toMap
    
    val projectArgs = 
      query.columnNames.
        flatMap( col => pickerColsTypeMap.get(col) match {
          case None => Some(ProjectArg(col, Var(col)))
          case Some(pickFromCol) => {
            if(projectedOutPicFromCols.contains(col))
              None //none if you dont want the from cols 
            else
              Some(ProjectArg(col, Var(col)))
          }
        }).union(Seq(ProjectArg(resultColName, db.compiler.optimize(pickExpr))))

    return (
      Project(projectArgs, query),
      Seq(pickerModel)
    )
  }
  

}

