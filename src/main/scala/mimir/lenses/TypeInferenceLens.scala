package mimir.lenses

import scala.util._
import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir.Database
import mimir.algebra._
import mimir.ctables._
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
      db.bestGuessSchema(query).map({
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
        db.interpreter.evalFloat(args(0)),
        1000,
        query
      )
    
    val columnIndexes = 
      modelColumns.zipWithIndex.toMap

    logger.debug(s"Training $model.name on $query")
    model.train(db, query)

    val repairs = 
      query.columnNames.map( col => {
        if(columnIndexes contains col){
          ProjectArg(col, 
            Function("CAST", Seq(
              Var(col),
              VGTerm(model.name, columnIndexes(col), Seq(), Seq())
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
