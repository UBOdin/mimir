package mimir.lenses;

import java.sql._

import mimir.Database
import mimir.models._
import mimir.algebra._
import mimir.ctables._
import mimir.parser._

object CommentLens {
  def create(
    db: Database, 
    name: String, 
    query: Operator, 
    args:Seq[Expression]
  ): (Operator, Seq[Model]) =
  {
    val colComments = args.
        map(field => {
          val split = db.interpreter.evalString(field).split(";", 2)
          val varName  = split(0).toUpperCase
          val varComment = split(1)
          (
            varName.toString.toUpperCase -> 
              varComment.toString
          )
        })
    val colCommentMap = colComments.toMap    
    val colCommentLists = colCommentMap.toSeq.unzip 
    val querySchemaLookup = db.typechecker.schemaOf(query).toMap
    val argTypesAndExprs = colComments.zipWithIndex.map {
      case (exprString, index) => {
        val commexpr = ExpressionParser.expr(exprString._1)
        (
          (commexpr, index),
          ("COMMENT_ARG_"+index, db.typechecker.typeOf(commexpr, scope = querySchemaLookup))
        )
      }
    }.unzip
    val modelSchema = argTypesAndExprs._2.unzip
    val model = new CommentModel(
      name + ":"+colCommentLists._1
        .mkString("_")
        .replaceAll("[ ,'\"()\\[\\]~!@#$%^&*<>?/\\|{}=;.-]", "")
        + Math.abs(colComments.mkString("_").hashCode()), 
      modelSchema._1, 
      modelSchema._2, 
      colCommentLists._2
    )
    val projArgs =  
      query.columnNames.map( col => {
          ProjectArg(col, Var(col))
      }).union(
          argTypesAndExprs._1.map(comExpr => 
            ProjectArg("COMMENT_ARG_"+comExpr._2, 
              VGTerm(model.name, /*comExpr._2*/0, Seq(RowIdVar()), Seq(comExpr._1)) ))
      )
    val oper = Project(projArgs, query)
    return (
      oper,
      Seq(model)
    )
  }
}
