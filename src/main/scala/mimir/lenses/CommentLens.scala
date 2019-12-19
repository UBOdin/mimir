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
    name: ID, 
    humanReadableName: String,
    query: Operator, 
    args:Seq[Expression]
  ): (Operator, Seq[Model]) =
  {
    val colComments = args.flatMap {
      case Function(ID("comment"), cols ) => 
        Some( cols match {
            case Seq(vcol@Var(col), StringPrimitive(comment)) => (vcol, comment, None)
            case Seq(expr:Expression, StringPrimitive(comment)) => (expr, comment, None)
            case Seq(vcol@Var(col), StringPrimitive(comment),StringPrimitive(rid)) => (vcol, comment, Some(rid))
            case Seq(expr:Expression, StringPrimitive(comment),StringPrimitive(rid)) => (expr, comment, Some(rid))
            case x => throw new RAException(s"No or bad comments specified for $name: $x")
          } )
      case _ => None
    }.toSeq 
  
    val modelName = ID(name + ":"+colComments.map(_._1)
        .mkString("_")
        .replaceAll("[ ,'\"()\\[\\]~!@#$%^&*<>?/\\|{}=;.-]", "")
        + Math.abs(colComments.mkString("_").hashCode())
      )

    val resultCols = args.flatMap {
      case Function(ID("result_columns"), cols:(Seq[Var] @unchecked)) => Some( cols.map(_.name) )
      case _ => None
    }.flatten
    
    
    val argTypesAndExprs = colComments.zipWithIndex.map {
      case ((expr, comment, rowIdOpt), index) => {
        val outputCol = 
          if(resultCols.length > index)
            resultCols(index)
          else
            ID("COMMENT_ARG_"+index)
        (
          ProjectArg(outputCol, rowIdOpt match { 
            case Some(rowid) => ExpressionUtils.makeCaseExpression( 
                  RowIdVar().eq(RowIdPrimitive(rowid)), 
                  Seq(
                    (BoolPrimitive(true), VGTerm(modelName, index, Seq(RowIdVar()), Seq(expr))),
                    (BoolPrimitive(false),expr)
                  ),
                  expr
                )
            case None => expr
          }
            ),
          (outputCol, db.typechecker.typeOf(expr, query)),
          comment
        )
      }
    }.unzip3
    
    val modelSchema = argTypesAndExprs._2.unzip
    val model = new CommentModel(
      modelName, 
      modelSchema._1, 
      modelSchema._2, 
      argTypesAndExprs._3
    )
    val projArgs =  
      query.columnNames
        .filterNot(col => resultCols.contains(col))
        .map( col => {
          ProjectArg(col, Var(col))
      }).union( argTypesAndExprs._1)
    val oper = Project(projArgs, query)
    return (
      oper,
      Seq(model)
    )
  }
}
