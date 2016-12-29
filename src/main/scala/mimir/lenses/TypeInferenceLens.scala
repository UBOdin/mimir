package mimir.lenses

import scala.util._
import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir.Database
import mimir.algebra._
import mimir.ctables._
import mimir.exec.ResultIterator
import mimir.models._

object TypeInferenceLens extends LazyLogging
{
  def create(
    db:Database, 
    name:String, 
    query:Operator, 
    args:Seq[Expression]
  ): (Operator, Seq[Model]) =
  {
    val modelColumns = 
      query.schema.map({
        case (col, (TString() | TAny())) => Some(col)
        case _ => None
      }).flatten.toIndexedSeq

    if(modelColumns.isEmpty){ 
      logger.warn("Type inference lens created on table with no string attributes")
      return (query, List())
    }

    if(args.isEmpty){
      throw new ModelException("Type inference lens requires a single parameter")
    }

    val model = 
      new TypeInferenceModel(
        name,
        modelColumns,
        Eval.evalFloat(args(0))
      )

    val columnIndexes = 
      modelColumns.zipWithIndex.toMap

    logger.debug(s"Training $model.name on $query")
    model.train(db, query)

    val repairs = 
      query.schema.map(_._1).map( col => {
        if(columnIndexes contains col){
          ProjectArg(col, 
            Function("CAST", List(
              Var(col),
              VGTerm(model, columnIndexes(col), List[Expression]())
            ))
          )
        } else {
          ProjectArg(col, Var(col))
        }
      })

    (
      Project(repairs, query),
      List(model)
    )
  }
}
