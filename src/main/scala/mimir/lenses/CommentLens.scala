package mimir.lenses;

import java.sql._

import mimir.Database
import mimir.models._
import mimir.algebra._
import mimir.ctables._

object CommentLens {
  def create(
    db: Database, 
    name: String, 
    query: Operator, 
    args:Seq[Expression]
  ): (Operator, Seq[Model]) =
  {
    val schema = query.schema
    val schemaMap = schema.toMap
    val colComments = args.
        map(field => {
          val split = Eval.evalString(field).split(";", 2)
          val varName  = split(0).toUpperCase
          val varComment = split(1)
          (
            varName.toString.toUpperCase -> 
              varComment.toString
          )
        })
    val colCommentMap = colComments.toMap    
    val colCommentLists = colCommentMap.toSeq.unzip 
    val argTypesAndExprs = colComments.zipWithIndex.map(arg => {
         val commexpr = db.operator.expr(arg._1._1)
         ((commexpr, arg._2), ("COMMENT_ARG_"+arg._2, Typechecker.typeOf(commexpr, query)))
    }).unzip
    val modelSchema = argTypesAndExprs._2.unzip
    val model : CommentModel = new CommentModel(name, modelSchema._1, modelSchema._2, colCommentLists._2)
    val projArgs =  
      query.schema.map(_._1).map( col => {
          ProjectArg(col, Var(col))
      }).union(
          argTypesAndExprs._1.map(comExpr => ProjectArg("COMMENT_ARG_"+comExpr._2, VGTerm(model, comExpr._2, Seq(comExpr._1), Seq()) ))
      )
    val oper = Project(projArgs, query)
    return (
      oper,
      Seq(model)
    )
  }
}
