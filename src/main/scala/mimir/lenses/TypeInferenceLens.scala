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
    val (repairs, models) = 
      query.schema.map({
        case (col, (TString() | TAny())) => {
          val model =
            new TypeInferenceModel(
              s"$name:$col",
              col,
              Eval.evalFloat(args(0))
            )
          logger.debug(s"Training $model.name on $query")
          model.train(db, query)

          val repair =
            ProjectArg(col, 
              Function("CAST", List(
                Var(col),
                VGTerm(model, 0, List[Expression]())
              ))
            )
          logger.trace(s"Going to repair $col as $repair")

          (repair, Some(model))
        }
        case (col, t) => {
          logger.debug(s"Don't need to cast $col of type $t")
          ( ProjectArg(col, Var(col)), None )
        }
      }).
      unzip

    (
      Project(repairs, query),
      models.flatten
    )
  }
}
