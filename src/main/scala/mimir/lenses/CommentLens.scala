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
    val lensComment = args.head.toString
    val colCommentLists = args.tail.
        map(field => {
          val split = Eval.evalString(field).split(" +", 2)
          val varName  = split(0).toUpperCase
          val varComment = split(1)
          (
            varName.toString.toUpperCase -> 
              varComment.toString
          )
        })
    
    val models = colCommentLists.map( colcom => {
      new CommentModel(s"$name:META:${colcom._1}", colcom._1, colcom._2)
    })

    return (
      query,
      models
    )
  }
}
