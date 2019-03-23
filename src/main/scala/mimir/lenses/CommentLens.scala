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
    query: Operator, 
    args:Seq[Expression]
  ): (Operator, Seq[Model]) =
  {
    val colComments = args.flatMap {
      case Function(ID("comment"), cols ) => 
        Some( cols match {
            case Seq(vcol@Var(col), StringPrimitive(comment)) => (vcol, comment)
            case Seq(expr:Expression, StringPrimitive(comment)) => (expr, comment)
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
      case ((expr, comment), index) => {
        val outputCol = 
          if(resultCols.length > index)
            resultCols(index)
          else
            ID("COMMENT_ARG_"+index)
        (
          ProjectArg(outputCol, VGTerm(modelName, index, Seq(RowIdVar()), Seq(expr))),
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
      query.columnNames.map( col => {
          ProjectArg(col, Var(col))
      }).union( argTypesAndExprs._1)
    val oper = Project(projArgs, query)
    return (
      oper,
      Seq(model)
    )
  }
}
