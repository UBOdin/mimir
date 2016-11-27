package mimir.lenses

import java.sql.SQLException
import scala.util._

import mimir.Database
import mimir.algebra.Type.T
import mimir.algebra._
import mimir.ctables._
import mimir.exec.ResultIterator
import mimir.util.TypeUtils
import mimir.models._

object TypeInferenceLens
{
  def create(
    db:Database, 
    name:String, 
    query:Operator, 
    args:List[Expression]
  ): (Operator, List[Model]) =
  {
    val (repairs, models) = 
      query.schema.map({
        case (col, Type.TString) => {
          val model =
            new TypeInferenceModel(
              s"$name:$col",
              col,
              Eval.evalFloat(args(0))
            )

          val repair =
            ProjectArg(col, 
              Function("CAST", List(
                Var(col),
                VGTerm(model, 0, List[Expression]())
              ))
            )

          (repair, Some(model))
        }
        case (col, _) =>
          ( ProjectArg(col, Var(col)), None )
      }).
      unzip

    (
      Project(repairs, query),
      models.flatten
    )
  }
}